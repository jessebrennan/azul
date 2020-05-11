from collections import defaultdict
import http
import json
import logging
import time
from typing import (
    List,
    MutableMapping,
)
import uuid

from boltons.cacheutils import cachedproperty
import boto3
import chalice
from chalice.app import (
    Request,
    SQSRecord,
)
from dataclasses import (
    dataclass,
    replace,
)
from more_itertools import chunked

from azul import (
    config,
    hmac,
)
from azul.azulclient import AzulClient
from azul.indexer.transformer import EntityReference
from azul.plugins import MetadataPlugin
from azul.types import JSON

log = logging.getLogger(__name__)


class IndexController:
    # The number of documents to be queued in a single SQS `send_messages`.
    # Theoretically, larger batches are better but SQS currently limits the
    # batch size to 10.
    #
    document_batch_size = 10

    def handle_notification(self, request: Request):
        hmac.verify(current_request=request)
        notification = request.json_body
        log.info("Received notification %r", notification)
        self._validate_notification(notification)
        if request.context['path'] == '/':
            result = self._handle_notification('add', notification)
        elif request.context['path'] in ('/delete', '/delete/'):
            result = self._handle_notification('delete', notification)
        else:
            assert False
        return result

    def _handle_notification(self, action: str, notification: JSON):
        if config.test_mode:
            if 'test_name' not in notification:
                log.error('Rejecting non-test notification in test mode: %r.', notification)
                raise chalice.ChaliceViewError('The indexer is currently in test mode where it only accepts specially '
                                               'instrumented notifications. Please try again later')
        else:
            if 'test_name' in notification:
                log.error('Rejecting test notification in production mode: %r.', notification)
                raise chalice.BadRequestError('Cannot process test notifications outside of test mode')

        message = dict(action=action, notification=notification)
        self._notify_queue.send_message(MessageBody=json.dumps(message))
        log.info("Queued notification %r", notification)
        return chalice.app.Response(body='', status_code=http.HTTPStatus.ACCEPTED)

    def _validate_notification(self, notification):
        try:
            match = notification['match']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: match')

        try:
            bundle_uuid = match['bundle_uuid']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_uuid')

        try:
            bundle_version = match['bundle_version']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_version')

        if not isinstance(bundle_uuid, str):
            raise chalice.BadRequestError(f'Invalid type: bundle_uuid: {type(bundle_uuid)} (should be str)')

        if not isinstance(bundle_version, str):
            raise chalice.BadRequestError(f'Invalid type: bundle_version: {type(bundle_version)} (should be str)')

        if bundle_uuid.lower() != str(uuid.UUID(bundle_uuid)).lower():
            raise chalice.BadRequestError(f'Invalid syntax: {bundle_uuid} (should be a UUID)')

        if not bundle_version:
            raise chalice.BadRequestError('Invalid syntax: bundle_version can not be empty')

    def contribute(self, event):
        for record in event:
            message = json.loads(record.body)
            attempts = record.to_dict()['attributes']['ApproximateReceiveCount']
            log.info(f'Worker handling message {message}, attempt #{attempts} (approx).')
            start = time.time()
            try:
                action = message['action']
                if action == 'reindex':
                    AzulClient.do_remote_reindex(message)
                else:
                    notification = message['notification']
                    indexer = self._create_indexer()
                    if action == 'add':
                        contributions = indexer.transform(notification, delete=False)
                    elif action == 'delete':
                        contributions = indexer.transform(notification, delete=True)
                    else:
                        assert False

                    log.info("Writing %i contributions to index.", len(contributions))
                    tallies = indexer.contribute(contributions)
                    tallies = [DocumentTally.for_entity(entity, num_contributions)
                               for entity, num_contributions in tallies.items()]

                    log.info("Queueing %i entities for aggregating a total of %i contributions.",
                             len(tallies), sum(tally.num_contributions for tally in tallies))
                    for batch in chunked(tallies, self.document_batch_size):
                        entries = [dict(tally.to_message(), Id=str(i)) for i, tally in enumerate(batch)]
                        self._document_queue.send_messages(Entries=entries)
            except BaseException:
                log.warning(f"Worker failed to handle message {message}.", exc_info=True)
                raise
            else:
                duration = time.time() - start
                log.info(f'Worker successfully handled message {message} in {duration:.3f}s.')

    def aggregate(self, event):
        # Consolidate multiple tallies for the same entity and process entities with only one message. Because SQS FIFO
        # queues try to put as many messages from the same message group in a reception batch, a single message per
        # group may indicate that that message is the last one in the group. Inversely, multiple messages per group
        # in a batch are a likely indicator for the presence of even more queued messages in that group. The more
        # bundle contributions we defer, the higher the amortized savings on aggregation become. Aggregating bundle
        # contributions is a costly operation for any entity with many contributions e.g., a large project.
        #
        tallies_by_id: MutableMapping[EntityReference, List[DocumentTally]] = defaultdict(list)
        for record in event:
            tally = DocumentTally.from_sqs_record(record)
            log.info('Attempt %i of handling %i contribution(s) for entity %s/%s',
                     tally.attempts, tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            tallies_by_id[tally.entity].append(tally)
        deferrals, referrals = [], []
        for tallies in tallies_by_id.values():
            if len(tallies) == 1:
                referrals.append(tallies[0])
            elif len(tallies) > 1:
                deferrals.append(tallies[0].consolidate(tallies[1:]))
            else:
                assert False
        if referrals:
            for tally in referrals:
                log.info('Aggregating %i contribution(s) to entity %s/%s',
                         tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            indexer = self._create_indexer()
            tallies = {tally.entity: tally.num_contributions for tally in referrals}
            indexer.aggregate(tallies)
        if deferrals:
            for tally in deferrals:
                log.info('Deferring aggregation of %i contribution(s) to entity %s/%s',
                         tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            entries = [dict(tally.to_message(), Id=str(i)) for i, tally in enumerate(deferrals)]
            self._document_queue.send_messages(Entries=entries)

    def _create_indexer(self):
        indexer_cls = self._plugin.indexer_class()
        indexer = indexer_cls()
        return indexer

    @cachedproperty
    def _plugin(self):
        return MetadataPlugin.load()

    @cachedproperty
    def _sqs(self):
        return boto3.resource('sqs')

    def _queue(self, queue_name):
        return self._sqs.get_queue_by_name(QueueName=queue_name)

    @property
    def _notify_queue(self):
        return self._queue(config.notify_queue_name)

    @property
    def _document_queue(self):
        return self._queue(config.document_queue_name)


@dataclass(frozen=True)
class DocumentTally:
    """
    Tracks the number of bundle contributions to a particular metadata entity.

    Each instance represents a message in the document queue.
    """
    entity: EntityReference
    num_contributions: int
    attempts: int

    @classmethod
    def from_sqs_record(cls, record: SQSRecord) -> 'DocumentTally':
        body = json.loads(record.body)
        attributes = record.to_dict()['attributes']
        return cls(entity=EntityReference(entity_type=body['entity_type'],
                                          entity_id=body['entity_id']),
                   num_contributions=body['num_contributions'],
                   attempts=int(attributes['ApproximateReceiveCount']))

    @classmethod
    def for_entity(cls, entity: EntityReference, num_contributions: int) -> 'DocumentTally':
        return cls(entity=entity,
                   num_contributions=num_contributions,
                   attempts=0)

    def to_json(self) -> JSON:
        return {
            'entity_type': self.entity.entity_type,
            'entity_id': self.entity.entity_id,
            'num_contributions': self.num_contributions
        }

    def to_message(self) -> JSON:
        return dict(MessageBody=json.dumps(self.to_json()),
                    MessageGroupId=self.entity.entity_id,
                    MessageDeduplicationId=str(uuid.uuid4()))

    def consolidate(self, others: List['DocumentTally']) -> 'DocumentTally':
        assert all(self.entity == other.entity for other in others)
        return replace(self, num_contributions=sum((other.num_contributions for other in others),
                                                   self.num_contributions))
