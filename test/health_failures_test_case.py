from contextlib import contextmanager
import os
import json
from unittest import mock
from uuid import uuid4

import boto3
from mock import (
    MagicMock,
    patch,
)
from more_itertools import chunked
from moto import (
    mock_sts,
    mock_sqs,
    mock_dynamodb2,
)
import requests

from app_test_case import LocalAppTestCase
from azul import config
from azul.health import HealthController
from azul.json_freeze import (
    freeze,
    sort_frozen,
)
from azul.modules import load_app_module
from retorts import ResponsesHelper


class TestHealthFailures(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    @contextmanager
    def _make_database(self):
        database = boto3.resource('dynamodb', region_name='us-east-1')
        table_settings = {
            'TableName': config.dynamo_failure_message_table_name,
            'KeySchema': [
                {
                    'AttributeName': 'MessageType',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'SentTimeMessageId',
                    'KeyType': 'RANGE'
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': 'MessageType',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'SentTimeMessageId',
                    'AttributeType': 'S'
                }
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        }
        database.create_table(**table_settings)
        with mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
            try:
                yield
            finally:
                table = database.Table(config.dynamo_failure_message_table_name)
                table.delete()

    @mock_sts
    @mock_sqs
    @mock_dynamodb2
    def test_failures_endpoint(self):
        indexer_app = load_app_module('indexer')
        sqs = boto3.resource('sqs', region_name='us-east-1')
        sqs.create_queue(QueueName=config.fail_queue_name)
        fail_queue = sqs.get_queue_by_name(QueueName=config.fail_queue_name)
        original_archive_fail_messages = HealthController.archive_fail_messages

        def mocked_archive_fail_messages(self):
            original_archive_fail_messages(self, 0)

        with patch.object(HealthController, 'archive_fail_messages', new=mocked_archive_fail_messages):
            with ResponsesHelper() as helper:
                helper.add_passthru(self.base_url)
                # The 4th sub-test checks if the indexer lambda can write more than 1 batch of messages to dynamodb.
                # The max number of messages in a batch is 10 and this sub-test populates the queue with 11 messages.
                for num_bundles, num_other in ((0, 0), (1, 0), (0, 1), (10, 1), (10, 0)):
                    with self._make_database():
                        bundle_notifications = [
                            {
                                'action': 'add',
                                'notification': self._fake_notification((str(uuid4()), '2019-10-14T113344.698028Z'))
                            } for _ in range(num_bundles)
                        ]
                        other_notifications = [{'other': 'notification'}] * num_other

                        for batch in chunked(bundle_notifications + other_notifications, 10):
                            items = [
                                {
                                    'Id': str(i), 'MessageBody': json.dumps(message)
                                } for i, message in enumerate(batch)
                            ]
                            fail_queue.send_messages(Entries=items)
                        expected_response = sort_frozen(freeze({
                            "failed_bundle_notifications": bundle_notifications,
                            "other_failed_messages": num_other
                        }))
                        with self.subTest(num_bundles=num_bundles,
                                          num_other=num_other):
                            indexer_app.retrieve_failure_messages(MagicMock(), MagicMock())
                            response = requests.get(self.base_url + '/health/failures')
                            self.assertEqual(200, response.status_code)
                            actual_response = sort_frozen(freeze(response.json()))
                            self.assertEqual(expected_response, actual_response)
