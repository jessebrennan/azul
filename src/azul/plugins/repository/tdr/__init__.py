from collections import (
    defaultdict,
)
from functools import (
    lru_cache,
)
import datetime
from itertools import (
    groupby,
)
import json
import logging
from operator import (
    itemgetter,
)
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    cast,
)
from urllib.parse import (
    unquote,
)

import attr
from furl import (
    furl,
)
import google.cloud.storage as gcs
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    RequirementError,
    cached_property,
    config,
    reject,
    require,
)
from azul.bigquery import (
    BigQueryRow,
    BigQueryRows,
)
from azul.deployment import (
    aws,
)
from azul.drs import (
    AccessMethod,
    DRSClient,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.indexer.document import (
    EntityID,
    EntityReference,
    EntityType,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.terra import (
    TDRClient,
    TDRSource,
    TerraDRSClient,
)
from azul.types import (
    JSON,
    JSONs,
    is_optional,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

Entities = Set[EntityReference]
EntitiesByType = Dict[EntityType, Set[EntityID]]


class Plugin(RepositoryPlugin):

    @classmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
        source = TDRSource.parse(config.tdr_source(catalog))
        return cls(source)

    def __init__(self, source: TDRSource) -> None:
        super().__init__()
        self._source = source

    @property
    def source(self) -> str:
        return str(self._source)

    @cached_property
    def tdr(self):
        return TDRClient()

    def list_bundles(self, prefix: str) -> List[BundleFQID]:
        log.info('Listing bundles in prefix %r.', prefix)
        bundle_ids = self.list_links_ids(prefix)
        log.info('Prefix %r contains %i bundle(s).', prefix, len(bundle_ids))
        return bundle_ids

    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        now = time.time()
        bundle = self.emulate_bundle(bundle_fqid)
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}

    def drs_uri(self, drs_path: str) -> str:
        netloc = furl(config.tdr_service_url).netloc
        return f'drs://{netloc}/{drs_path}'

    def direct_file_url(self,
                        file_uuid: str,
                        *,
                        file_version: Optional[str] = None,
                        replica: Optional[str] = None
                        ) -> Optional[str]:
        return None

    @classmethod
    def format_version(cls, version: datetime.datetime) -> str:
        return version.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def _run_sql(self, query):
        return self.tdr.run_sql(query)

    def _full_table_name(self, table_name: str) -> str:
        return self._source.qualify_table(table_name)

    def list_links_ids(self, prefix: str) -> List[BundleFQID]:
        validate_uuid_prefix(prefix)
        current_bundles = self._query_latest_version(f'''
            SELECT links_id, version
            FROM {self._full_table_name('links')}
            WHERE STARTS_WITH(links_id, '{prefix}')
        ''', group_by=('links_id',))
        return [BundleFQID(uuid=row['links_id'],
                           version=self.format_version(row['version']))
                for row in current_bundles]

    def _query_latest_version(self,
                              query: str,
                              group_by: Sequence[str]
                              ) -> List[BigQueryRow]:
        assert not isinstance(group_by, str), \
            "Use `group_by=('foo',)`, not `group_by='foo'`"
        iter_rows = self._run_sql(query)
        key = itemgetter(*group_by)
        groups = groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(group) for _, group in groups]

    def _choose_one_version(self, versioned_items: BigQueryRows) -> BigQueryRow:
        if self._source.is_snapshot:
            return one(versioned_items)
        else:
            return max(versioned_items, key=itemgetter('version'))

    def emulate_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        bundle = TDRBundle(source=self._source,
                           uuid=bundle_fqid.uuid,
                           version=bundle_fqid.version,
                           manifest=[],
                           metadata_files={})
        entities, links_jsons = self._stitch_bundles(bundle_fqid)
        bundle.add_entity('links.json', self._merge_links(links_jsons))
        # Already sorted by entity_type
        for entity_type, rows in groupby(self._retrieve_entities(entities),
                                         itemgetter('entity_type')):
            for i, row in enumerate(rows):
                bundle.add_entity(f'{entity_type}_{i}.json', row)

        return bundle

    def _stitch_bundles(self,
                        root_bundle: BundleFQID
                        ) -> Tuple[EntitiesByType, List[JSON]]:
        """
        Recursively follow dangling inputs to collect entities from upstream
        bundles, ensuring that no bundle is processed more than once.
        """
        entities: EntitiesByType = defaultdict(set)
        unprocessed: Set[BundleFQID] = {root_bundle}
        processed: Set[BundleFQID] = set()
        links_jsons: List[JSON] = []
        while unprocessed:
            bundle = unprocessed.pop()
            processed.add(bundle)
            links_json = self._retrieve_links(bundle)
            links_jsons.append(links_json)
            links = links_json['content']['links']
            project = EntityReference(entity_type='project',
                                      entity_id=links_json['project_id'])
            (
                bundle_entities,
                inputs,
                outputs,
                supplementary_files
            ) = self._parse_links(links, project)
            for entity in bundle_entities:
                entities[entity.entity_type].add(entity.entity_id)

            dangling_inputs = self._dangling_inputs(inputs, outputs | supplementary_files)
            if dangling_inputs:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug('Bundle %r has dangling inputs: %r', bundle, dangling_inputs)
                else:
                    log.info('Bundle %r has %i dangling inputs', bundle, len(dangling_inputs))
                unprocessed |= self._find_upstream_bundles(dangling_inputs) - processed
            else:
                log.debug('Bundle %r is self-contained', bundle)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Stitched together bundles: %r', processed)
        else:
            log.info('Stitched together %i bundles', len(processed))
        return entities, links_jsons

    def _retrieve_links(self, links_id: BundleFQID) -> JSON:
        """
        Retrieve a links entity from BigQuery and parse the `content` column.

        :param links_id: Which links entity to retrieve.
        """
        links = one(self._run_sql(f'''
            SELECT {", ".join(TDRBundle.columns('links'))}
            FROM {self._full_table_name('links')}
            WHERE links_id = '{links_id.uuid}'
                AND version = TIMESTAMP('{links_id.version}')
        '''))
        links = dict(links)  # Enable item assignment to pre-parse content JSON
        links['content'] = json.loads(links['content'])
        return links

    def _retrieve_entities(self,
                           entities: EntitiesByType,
                           ) -> BigQueryRows:
        metadata_subqueries = []
        file_subqueries = []
        for entity_type, entity_ids in entities.items():
            log.debug('Retrieving %i entities of type %r ...', len(entity_ids), entity_type)
            (file_subqueries
             if entity_type.endswith('_file')
             else metadata_subqueries).append(f'''(
                SELECT {', '.join(TDRBundle.columns(entity_type))}
                FROM {self._full_table_name(entity_type)}
                WHERE {' OR '.join(f"{entity_type}_id = '{entity_id}'"
                                   for entity_id in entity_ids)}
            )''')
        rows = []
        for subqueries in (metadata_subqueries, file_subqueries):
            rows.extend(self._query_latest_version('UNION ALL'.join(subqueries),
                                                   group_by=('entity_type', 'entity_id')))
        retrieved_ids = {row['entity_id'] for row in rows}
        for entity_type, entity_ids in entities.items():
            log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
            missing = entity_ids - retrieved_ids
            require(not missing,
                    f'Required {entity_type} entities not found in {self._source}: {missing}')
        return rows

    def _parse_links(self,
                     links: JSONs,
                     project: EntityReference
                     ) -> Tuple[Entities, Entities, Entities, Entities]:
        """
        Collects inputs, outputs, and other entities referenced in the bundle
        links.

        :param links: The "links" property of a links.json file.

        :param project: The project for the bundle defined by these links.

        :return: A tuple of (1) a set of all entities found in the links, (2)
                 the subset of those entities that occur as inputs, (3)
                 those that occur as outputs, (4) those that occur as
                 supplementary files.
        """
        entities = set()
        inputs = set()
        outputs = set()
        supplementary_files = set()
        entities.add(project)
        for link in links:
            link_type = link['link_type']
            if link_type == 'process_link':
                process = EntityReference(entity_type=link['process_type'],
                                          entity_id=link['process_id'])
                entities.add(process)
                for category in ('input', 'output', 'protocol'):
                    for entity in cast(JSONs, link[category + 's']):
                        entity = EntityReference(entity_type=entity[category + '_type'],
                                                 entity_id=entity[category + '_id'])
                        entities.add(entity)
                        if category == 'input':
                            inputs.add(entity)
                        elif category == 'output':
                            outputs.add(entity)
            elif link_type == 'supplementary_file_link':
                associate = EntityReference(entity_type=link['entity']['entity_type'],
                                            entity_id=link['entity']['entity_id'])
                # For MVP, only project entities can have associated supplementary files.
                require(associate == project,
                        'Supplementary file must be associated with the current project',
                        project, associate)
                for entity in cast(JSONs, link['files']):
                    entity = EntityReference(entity_type='supplementary_file',
                                             entity_id=entity['file_id'])
                    entities.add(entity)
                    supplementary_files.add(entity)
            else:
                raise RequirementError('Unexpected link_type', link_type)
        return entities, inputs, outputs, supplementary_files

    def _dangling_inputs(self, inputs: Entities, outputs: Entities) -> Entities:
        return {
            input
            for input in inputs
            if input.entity_type.endswith('_file') and input not in outputs
        }

    def _find_upstream_bundles(self, outputs: Entities) -> Set[BundleFQID]:
        """
        Search for bundles containing processes that produce the specified output
        entities.
        """
        output_ids = [output.entity_id for output in outputs]
        output_id = 'JSON_EXTRACT_SCALAR(link_output, "$.output_id")'
        rows = self._run_sql(f'''
            SELECT links_id, version, {output_id} AS output_id
            FROM {self._full_table_name('links')} AS links
                JOIN UNNEST(JSON_EXTRACT_ARRAY(links.content, '$.links')) AS content_links
                    ON JSON_EXTRACT_SCALAR(content_links, '$.link_type') = 'process_link'
                JOIN UNNEST(JSON_EXTRACT_ARRAY(content_links, '$.outputs')) AS link_output
                    ON {output_id} IN UNNEST({output_ids})
        ''')
        bundles = set()
        outputs_found = set()
        for row in rows:
            bundles.add(BundleFQID(uuid=row['links_id'],
                                   version=self.format_version(row['version'])))
            outputs_found.add(row['output_id'])
        missing = set(output_ids) - outputs_found
        require(not missing,
                f'Dangling inputs not found in any bundle: {missing}')
        return bundles

    def _merge_links(self, links_jsons: JSONs) -> JSON:
        """
        Merge the links.json documents from multiple stitched bundles into a
        single document.
        """
        root, *stitched = links_jsons
        if stitched:
            merged = {'entity_id': root['entity_id'],
                      'version': root['version']}
            for common_key in ('entity_type', 'project_id', 'schema_type'):
                merged[common_key] = one({row[common_key] for row in links_jsons})
            merged_content = {}
            source_contents = [row['content'] for row in links_jsons]
            for common_key in ('describedBy', 'schema_type', 'schema_version'):
                merged_content[common_key] = one({sc[common_key] for sc in source_contents})
            merged_content['links'] = sum((sc['links'] for sc in source_contents),
                                          start=[])
            merged['content'] = merged_content  # Keep result of parsed JSON for reuse
            merged['content_size'] = len(json.dumps(merged_content))
            keys = one({frozenset(row.keys()) for row in links_jsons})
            assert merged.keys() == keys, (merged.keys(), keys)
            content_keys = one({frozenset(sc.keys()) for sc in source_contents})
            assert merged_content.keys() == content_keys, (merged_content.keys(),
                                                           content_keys)
            return merged
        else:
            return root

    def drs_client(self) -> DRSClient:
        return TerraDRSClient()

    def file_download_class(self) -> Type[RepositoryFileDownload]:
        return TDRFileDownload


class TDRFileDownload(RepositoryFileDownload):

    def _get_blob(self, bucket_name: str, blob_name: str) -> gcs.Blob:
        """
        Get a Blob object by name.
        """
        with aws.service_account_credentials():
            client = gcs.Client()
        bucket = gcs.Bucket(client, bucket_name)
        return bucket.get_blob(blob_name)

    _location: Optional[str] = None

    def update(self, plugin: RepositoryPlugin) -> None:
        require(self.replica is None or self.replica == 'gcp')
        assert self.drs_path is not None
        drs_uri = plugin.drs_uri(self.drs_path)
        drs_client = plugin.drs_client()
        access = drs_client.get_object(drs_uri, access_method=AccessMethod.gs)
        assert access.headers is None
        url = furl(access.url)
        blob_name = '/'.join(url.path.segments)
        # https://github.com/databiosphere/azul/issues/2479#issuecomment-733410253
        if url.fragmentstr:
            blob_name += '#' + unquote(url.fragmentstr)
        else:
            # furl does not differentiate between no fragment and empty
            # fragment
            if access.url.endswith('#'):
                blob_name += '#'
        blob = self._get_blob(bucket_name=url.netloc, blob_name=blob_name)
        expiration = int(time.time() + 3600)
        file_name = self.file_name.replace('"', r'\"')
        assert all(0x1f < ord(c) < 0x80 for c in file_name)
        disposition = f"attachment; filename={file_name}"
        signed_url = blob.generate_signed_url(expiration=expiration,
                                              response_disposition=disposition)
        self._location = signed_url

    @property
    def location(self) -> Optional[str]:
        return self._location

    @property
    def retry_after(self) -> Optional[int]:
        return None


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Checksums:
    crc32c: str
    sha1: Optional[str] = None
    sha256: str
    s3_etag: Optional[str] = None

    def to_json(self) -> Dict[str, str]:
        """
        >>> Checksums(crc32c='a', sha1='b', sha256='c', s3_etag=None).to_json()
        {'crc32c': 'a', 'sha1': 'b', 'sha256': 'c'}
        """
        return {k: v for k, v in attr.asdict(self).items() if v is not None}

    @classmethod
    def from_json(cls, json: JSON) -> 'Checksums':
        """
        >>> Checksums.from_json({'crc32c': 'a', 'sha256': 'c'})
        Checksums(crc32c='a', sha1=None, sha256='c', s3_etag=None)

        >>> Checksums.from_json({'crc32c': 'a', 'sha1':'b', 'sha256': 'c', 's3_etag': 'd'})
        Checksums(crc32c='a', sha1='b', sha256='c', s3_etag='d')

        >>> Checksums.from_json({'crc32c': 'a'})
        Traceback (most recent call last):
            ...
        ValueError: ('JSON property cannot be absent or null', 'sha256')
        """

        def extract_field(field: attr.Attribute) -> Tuple[str, Any]:
            value = json.get(field.name)
            if value is None and not is_optional(field.type):
                raise ValueError('JSON property cannot be absent or null', field.name)
            return field.name, value

        return cls(**dict(map(extract_field, attr.fields(cls))))


@attr.s(auto_attribs=True, kw_only=True)
class TDRBundle(Bundle):
    source: TDRSource

    def add_entity(self, entity_key: str, entity_row: BigQueryRow) -> None:
        schema_type = entity_row['schema_type']
        self._add_manifest_entry(name=entity_key,
                                 uuid=entity_row['entity_id'],
                                 version=Plugin.format_version(entity_row['version']),
                                 size=entity_row['content_size'],
                                 content_type='application/json',
                                 dcp_type=f'"metadata/{schema_type}"')
        if schema_type == 'file':
            descriptor = json.loads(entity_row['descriptor'])
            self._add_manifest_entry(name=entity_row['file_name'],
                                     uuid=descriptor['file_id'],
                                     version=descriptor['file_version'],
                                     size=descriptor['size'],
                                     content_type=descriptor['content_type'],
                                     dcp_type='data',
                                     checksums=Checksums.from_json(descriptor),
                                     drs_path=self._parse_file_id_column(entity_row['file_id']))
        content = entity_row['content']
        self.metadata_files[entity_key] = (json.loads(content)
                                           if isinstance(content, str)
                                           else content)

    @classmethod
    @lru_cache
    def columns(cls, entity_type: EntityType) -> Sequence[str]:
        # BigQuery UNION combines columns based on *order*, not name, so these
        # must have consistent order.
        return (
            f'{entity_type}_id AS entity_id',
            f'"{entity_type}" AS entity_type',
            'version',
            'JSON_EXTRACT_SCALAR(content, "$.schema_type") AS schema_type',
            'BYTE_LENGTH(content) AS content_size',
            'content',
            *((
                  'project_id',
              ) if entity_type == 'links' else (
                'descriptor',
                'JSON_EXTRACT_SCALAR(content, "$.file_core.file_name") AS file_name',
                'file_id'
            ) if entity_type.endswith('_file') else ())
        )

    def drs_path(self, manifest_entry: JSON) -> Optional[str]:
        return manifest_entry.get('drs_path')

    def _add_manifest_entry(self,
                            *,
                            name: str,
                            uuid: str,
                            version: str,
                            size: int,
                            content_type: str,
                            dcp_type: str,
                            checksums: Optional[Checksums] = None,
                            drs_path: Optional[str] = None) -> None:
        self.manifest.append({
            'name': name,
            'uuid': uuid,
            'version': version,
            'content-type': f'{content_type}; dcp-type={dcp_type}',
            'size': size,
            **(
                {
                    'indexed': False,
                    'drs_path': drs_path,
                    **checksums.to_json()
                } if dcp_type == 'data' else {
                    'indexed': True,
                    'crc32c': '',
                    'sha256': ''
                }
            )
        })

    def _parse_file_id_column(self, file_id: Optional[str]) -> Optional[str]:
        # The file_id column is present for datasets, but is usually null, may
        # contain unexpected/unusable values, and NEVER produces usable DRS URLs,
        # so we avoid parsing the column altogether for datasets.
        if self.source.is_snapshot:
            reject(file_id is None)
            # TDR stores the complete DRS URI in the file_id column, but we only
            # index the path component. These requirements prevent mismatches in
            # the DRS domain, and ensure that changes to the column syntax don't
            # go undetected.
            file_id = furl(file_id)
            require(file_id.scheme == 'drs')
            require(file_id.netloc == furl(config.tdr_service_url).netloc)
            return str(file_id.path).strip('/')
        else:
            return None
