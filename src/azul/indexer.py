from abc import ABC, abstractmethod
from collections import Counter, defaultdict
import logging
from operator import attrgetter
from typing import Iterable, List, Mapping, MutableMapping, MutableSet, Union

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, scan, streaming_bulk
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
from more_itertools import one

from azul import config
from azul.es import ESClientFactory
from azul.transformer import (Aggregate,
                              AggregatingTransformer,
                              Contribution,
                              Document,
                              DocumentCoordinates,
                              EntityReference,
                              Transformer)
from azul.types import JSON

log = logging.getLogger(__name__)

Tallies = Mapping[EntityReference, int]


class BaseIndexer(ABC):
    """
    The base indexer class provides the framework to do indexing.
    """

    @abstractmethod
    def mapping(self) -> JSON:
        raise NotImplementedError()

    @abstractmethod
    def settings(self) -> JSON:
        raise NotImplementedError()

    @abstractmethod
    def transformers(self) -> Iterable[Transformer]:
        raise NotImplementedError()

    @abstractmethod
    def entities(self) -> Iterable[str]:
        raise NotImplementedError()

    def index_names(self, aggregate=None) -> List[str]:
        aggregates = (False, True) if aggregate is None else (aggregate,)
        return [config.es_index_name(entity, aggregate=aggregate)
                for entity in self.entities()
                for aggregate in aggregates]

    def index(self, writer: 'IndexWriter', dss_notification: JSON) -> None:
        """
        Index the bundle referenced by the given notification. This is an inefficient default implementation. A more
        efficient implementation would transform many bundles, collect their contributions and aggregate all affected
        entities at the end.
        """
        contributions = self.transform(dss_notification)
        tallies = self.contribute(writer, contributions)
        self.aggregate(writer, tallies)

    def transform(self, dss_notification: JSON) -> List[Contribution]:
        """
        Transform the metadata in the bundle referenced by the given notification into a list of contributions to
        documents, each document representing one metadata entity in the index.
        """
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        manifest, metadata_files = self._get_bundle(bundle_uuid, bundle_version)

        # FIXME: this seems out of place. Consider creating indices at deploy time and avoid the mostly
        # redundant requests for every notification (https://github.com/DataBiosphere/azul/issues/427)
        es_client = ESClientFactory.get()
        for index_name in self.index_names():
            es_client.indices.create(index=index_name,
                                     ignore=[400],
                                     body=dict(settings=self.settings(),
                                               mappings=dict(doc=self.mapping())))
        contributions = []
        for transformer in self.transformers():
            contributions.extend(transformer.transform(uuid=bundle_uuid,
                                                       version=bundle_version,
                                                       manifest=manifest,
                                                       metadata_files=metadata_files))
        return contributions

    def _get_bundle(self, bundle_uuid, bundle_version):
        _, manifest, metadata_files = download_bundle_metadata(client=config.dss_client(),
                                                               replica='aws',
                                                               uuid=bundle_uuid,
                                                               version=bundle_version,
                                                               num_workers=config.num_dss_workers)
        assert _ == bundle_version
        return manifest, metadata_files

    def contribute(self, writer: 'IndexWriter', contributions: List[Contribution]) -> Tallies:
        """
        Write the given entity contributions to the index and return tallies, a dictionary tracking the number of
        contributions made to each entity.
        """
        while True:
            writer.write(contributions)
            if not writer.retries:
                break
            contributions = [v for v in contributions if v.coordinates in writer.retries]
        writer.raise_on_errors()
        tallies = Counter(c.entity for c in contributions)
        return tallies

    def aggregate(self, writer: 'IndexWriter', tallies: Tallies):
        """
        Read all contributions to the entities listed in the given tallies from the index, aggregate the
        contributions into one aggregate per entity and write the resulting aggregates to the index.
        """
        while True:
            # Read the aggregates
            # FIXME: we should source filter so we only read the num_contributions and version values
            old_aggregates = self._read_aggregates(entity for entity in tallies.keys())
            absolute_tallies = Counter(tallies)
            absolute_tallies.update({old_aggregate.entity: old_aggregate.num_contributions
                                     for old_aggregate in old_aggregates.values()})
            # Read all contributions from Elasticsearch
            contributions = self._read_contributions(absolute_tallies)
            # Combine the contributions into old_aggregates, one per entity
            new_aggregates = self._aggregate(contributions)
            # Set the expected document version from the old version
            for new_aggregate in new_aggregates:
                old_aggregate = old_aggregates.pop(new_aggregate.entity, None)
                new_aggregate.version = None if old_aggregate is None else old_aggregate.version
            # Empty out any unreferenced aggregates (can only happen for deletions)
            for old_aggregate in old_aggregates.values():
                old_aggregate.contents = {}
                new_aggregates.append(old_aggregate)
            # Write aggregates to Elasticsearch
            writer.write(new_aggregates)
            # Retry if necessary
            if not writer.retries:
                break
            tallies = {aggregate.entity: tallies[aggregate.entity]
                       for aggregate in new_aggregates
                       if aggregate.coordinates in writer.retries}
        writer.raise_on_errors()

    def _read_aggregates(self, entities: Iterable[EntityReference]) -> MutableMapping[EntityReference, Aggregate]:
        request = dict(docs=[dict(_type=Aggregate.type,
                                  _index=Aggregate.index_name(entity.entity_type),
                                  _id=entity.entity_id)  # FIXME: assumes that document_id is entity_id for aggregates
                             for entity in entities])
        response = ESClientFactory.get().mget(body=request)
        aggregates = (Aggregate.from_index(doc) for doc in response['docs'] if doc['found'])
        aggregates = {a.entity: a for a in aggregates}
        return aggregates

    def _read_contributions(self, tallies: Tallies) -> List[Contribution]:
        es_client = ESClientFactory.get()
        query = {
            "query": {
                "terms": {
                    "entity_id.keyword": [e.entity_id for e in tallies.keys()]
                }
            }
        }
        index = sorted(list({config.es_index_name(e.entity_type, aggregate=False) for e in tallies.keys()}))
        # scan() uses a server-side cursor and is expensive. Only use it if the number of contributions is large
        page_size = 100
        num_contributions = sum(tallies.values())
        hits = None
        if num_contributions <= page_size:
            log.info('Reading %i expected contribution(s) using search().', num_contributions)
            response = es_client.search(index=index, body=query, size=page_size, doc_type=Document.type)
            total_hits = response['hits']['total']
            if total_hits <= page_size:
                hits = response['hits']['hits']
                assert len(hits) == total_hits
            else:
                log.info('Expected only %i contribution(s) but got %i.', num_contributions, total_hits)
                num_contributions = total_hits
        if hits is None:
            log.info('Reading %i expected contribution(s) using scan().', num_contributions)
            hits = scan(es_client, index=index, query=query, size=page_size, doc_type=Document.type)
        contributions = [Contribution.from_index(hit) for hit in hits]
        log.info('Read %i contribution(s).', len(contributions))
        return contributions

    def _aggregate(self, contributions: List[Contribution]) -> List[Aggregate]:
        # Group contributions by entity ID and bundle UUID
        contributions_by_bundle = defaultdict(list)
        tallies = Counter()
        for contribution in contributions:
            contributions_by_bundle[contribution.entity.entity_id, contribution.bundle_uuid].append(contribution)
            # Track the raw, unfiltered number of contributions per entity
            tallies[contribution.entity.entity_id] += 1

        # Among the contributions by a particular bundle to a particular entity, select the contribution from latest
        # version of that bundle. If the latest version is a deletion, take no contribution from that bundle. Group
        # selected contributions by entity type and ID.
        contributions_by_entity = defaultdict(list)
        for bundle_contributions in contributions_by_bundle.values():
            contribution = max(bundle_contributions, key=attrgetter('bundle_version'))
            if not contribution.bundle_deleted:
                contributions_by_entity[contribution.entity].append(contribution)

        # Create lookup for transformer by entity type
        transformers = {t.entity_type(): t for t in self.transformers() if isinstance(t, AggregatingTransformer)}

        # Aggregate contributions for the same entity
        aggregates = []
        for entity, contributions in contributions_by_entity.items():
            transformer = transformers[entity.entity_type]
            contents = transformer.aggregate(contributions)
            bundles = [dict(uuid=c.bundle_uuid, version=c.bundle_version) for c in contributions]
            # noinspection PyArgumentList
            # https://youtrack.jetbrains.com/issue/PY-28506
            aggregate = Aggregate(entity=entity,
                                  version=None,
                                  contents=contents,
                                  bundles=bundles,
                                  num_contributions=tallies[entity.entity_id])
            aggregates.append(aggregate)

        return aggregates

    def delete(self, writer: 'IndexWriter', dss_notification: JSON) -> None:
        # FIXME: this only works if the bundle version is not being indexed concurrently
        # The fix could be to optimistically lock on the aggregate version
        contributions = self.transform_deletion(dss_notification)
        # FIXME: these are all modified contributions, not new ones. This also happens when we reindex without
        # deleting the indices first. The tallies refer to number of updated or added contributions but we treat them
        # as if they are all new when we estimate the number of contributions per bundle.
        tallies = self.contribute(writer, contributions)
        self.aggregate(writer, tallies)

    def transform_deletion(self, dss_notification: JSON) -> List[Contribution]:
        match = dss_notification['match']
        es_client = ESClientFactory.get()
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"bundle_uuid.keyword": match['bundle_uuid']}},
                        {"term": {"bundle_version.keyword": match['bundle_version']}}
                    ]
                }
            }
        }
        # FIXME: decide if we want to keep the contents of the contribution, or not (and use source filtering to
        # minimize the response size)
        response = es_client.search(body=query,
                                    doc_type="doc",
                                    index=self.index_names(aggregate=False))
        contributions = []
        for hit in response['hits']['hits']:
            contribution = Contribution.from_index(hit)
            contribution.bundle_deleted = True
            contributions.append(contribution)

        return contributions


class IndexWriter:
    def __init__(self,
                 refresh: Union[bool, str],
                 conflict_retry_limit: int,
                 error_retry_limit: int) -> None:
        """
        :param refresh: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/docs-refresh.html

        :param conflict_retry_limit: The maximum number of retries (the second attempt is the first retry) on version
                                     conflicts. Specify 0 for no retries or None for unlimited retries.

        :param error_retry_limit: The maximum number of retries (the second attempt is the first retry) on other
                                  errors. Specify 0 for no retries or None for unlimited retries.
        """
        super().__init__()
        self.refresh = refresh
        self.conflict_retry_limit = conflict_retry_limit
        self.error_retry_limit = error_retry_limit
        self.es_client = ESClientFactory.get()
        self.errors: MutableMapping[DocumentCoordinates, int] = defaultdict(int)
        self.conflicts: MutableMapping[DocumentCoordinates, int] = defaultdict(int)
        self.retries: MutableSet[DocumentCoordinates] = None

    def write(self, documents: List[Document]):
        # documents.sort(key=attrgetter('coordinates'))
        self.retries = set()
        if len(documents) <= 32:
            self._write_individually(documents)
        else:
            self._write_bulk(documents)

    def _write_individually(self, documents: Iterable[Document]):
        log.info('Writing documents individually')
        for doc in documents:
            try:
                self.es_client.index(refresh=self.refresh, **doc.to_index())
            except ConflictError as e:
                self._on_conflict(doc, e)
            except ElasticsearchException as e:
                self._on_error(doc, e)
            else:
                self._on_success(doc)

    def _write_bulk(self, documents: Iterable[Document]):
        documents: Mapping[DocumentCoordinates, Document] = {doc.coordinates: doc for doc in documents}
        actions = [doc.to_index(bulk=True) for doc in documents.values()]
        if len(actions) < 1024:
            log.info('Writing documents using streaming_bulk().')
            helper = streaming_bulk
        else:
            log.info('Writing documents using parallel_bulk().')
            helper = parallel_bulk
        response = helper(client=self.es_client,
                          actions=actions,
                          refresh=self.refresh,
                          raise_on_error=False,
                          max_chunk_bytes=10485760)
        for success, info in response:
            op_type, info = one(info.items())
            assert op_type in ('index', 'create')
            coordinates = DocumentCoordinates(document_index=info['_index'], document_id=info['_id'])
            doc = documents[coordinates]
            if success:
                self._on_success(doc)
            else:
                if info['index']['status'] == 409:
                    self._on_conflict(doc, info)
                else:
                    self._on_error(doc, info)

    def _on_success(self, doc: Document):
        coordinates = doc.coordinates
        self.conflicts.pop(coordinates, None)
        self.errors.pop(coordinates, None)
        if isinstance(doc, Aggregate):
            log.debug('Successfully wrote document %s/%s with %i contribution(s).',
                      coordinates.document_index, coordinates.document_id, doc.num_contributions)
        else:
            log.debug('Successfully wrote document %s/%s.',
                      coordinates.document_index, coordinates.document_id)

    def _on_error(self, doc: Document, e):
        self.errors[doc.coordinates] += 1
        if self.error_retry_limit is None or self.errors[doc.coordinates] <= self.error_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        log.warning('There was a general error with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.errors[doc.coordinates], action)

    def _on_conflict(self, doc: Document, e):
        self.conflicts[doc.coordinates] += 1
        self.errors.pop(doc.coordinates, None)  # a conflict resets the error count
        if self.conflict_retry_limit is None or self.conflicts[doc.coordinates] <= self.conflict_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        log.warning('There was a conflict with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.conflicts[doc.coordinates], action)

    def raise_on_errors(self):
        if self.errors or self.conflicts:
            log.warning('Failures: %r', self.errors)
            log.warning('Conflicts: %r', self.conflicts)
            raise RuntimeError('Failed to index documents. Failures: %i, conflicts: %i.' %
                               (len(self.errors), len(self.conflicts)))
