import io
import json
import logging
import os
import time
from unittest import (
    mock,
)
from unittest.mock import (
    MagicMock,
)

import certifi
from chalice.config import (
    Config as ChaliceConfig,
)
from furl import (
    furl,
)
from google.auth.transport.urllib3 import (
    AuthorizedHttp,
)
from moto import (
    mock_s3,
)
import requests
import responses
import urllib3

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.drs import (
    Access,
    AccessMethod,
    DRSClient,
)
from azul.http import (
    http_client,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins.repository.tdr import (
    TDRFileDownload,
)
from azul.service.index_query_service import (
    IndexQueryService,
)
from azul.terra import (
    TerraClient,
)
from retorts import (
    ResponsesHelper,
)
from service import (
    DSSUnitTestCase,
)

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(logger)


# These are the credentials defined in moto.instance_metadata.responses.InstanceMetadataResponse which, for reasons
# yet to be determined, is used on Travis but not when I run this locally. Maybe it's the absence of
# ~/.aws/credentials. The credentials that @mock_sts provides look more realistic but boto3's STS credential provider
# would be skipped on CI because the lack of ~/.aws/credentials there implies that AssumeRole credentials aren't
# configured, causing boto3 to default to use credentials from mock instance metadata.
#
mock_access_key_id = 'test-key'  # @mock_sts uses AKIAIOSFODNN7EXAMPLE
mock_secret_access_key = 'test-secret-key'  # @mock_sts uses wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY
mock_session_token = 'test-session-token'  # @mock_sts token starts with  AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk …

mock_tdr_service_url = f'https://serpentine.datarepo-dev.broadinstitute.net.test.{config.domain_name}'
mock_tdr_sources = 'tdr:mock:snapshot/mock_snapshot'


class RepositoryPluginTestCase(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    def chalice_config(self):
        return ChaliceConfig.create(lambda_timeout=15)

    def assertUrlEqual(self, a: furl, b: furl):
        if isinstance(a, str):
            a = furl(a)
        if isinstance(b, str):
            b = furl(b)
        self.assertEqual(a.scheme, b.scheme)
        self.assertEqual(a.username, b.username)
        self.assertEqual(a.password, b.password)
        self.assertEqual(a.host, b.host)
        self.assertEqual(a.port, b.port)
        self.assertEqual(a.path, b.path)
        self.assertEqual(sorted(a.args.allitems()), sorted(b.args.allitems()))


class TestTDRRepositoryProxy(RepositoryPluginTestCase):
    catalog = 'test-tdr'
    catalog_config = f'hca:{catalog}:metadata/hca:repository/tdr'

    @mock.patch.dict(os.environ,
                     AZUL_TDR_SERVICE_URL=mock_tdr_service_url,
                     AZUL_TDR_SOURCES=mock_tdr_sources)
    @mock.patch.object(TerraClient,
                       'oauthed_http',
                       AuthorizedHttp(MagicMock(),
                                      urllib3.PoolManager(ca_certs=certifi.where())))
    def test_repository_files_proxy(self):
        client = http_client()

        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T121154.054628Z'
        organic_file_name = 'foo.txt'
        drs_path_id = 'v1_c99baa6f-24ce-4837-8c4a-47ca4ec9d292_b967ecc9-98b2-43c6-8bac-28c0a4fa7812'
        file_doc = {
            'name': organic_file_name,
            'version': file_version,
            'drs_path': drs_path_id,
            'size': 1,
        }
        for fetch in True, False:
            with self.subTest(fetch=fetch):
                with mock.patch.object(IndexQueryService,
                                       'get_data_file',
                                       return_value=file_doc):
                    azul_url = furl(
                        url=self.base_url,
                        path=['fetch' if fetch else '', 'repository', 'files', file_uuid],
                        args={
                            'catalog': self.catalog,
                            'version': file_version,
                        })

                    gs_bucket_name = 'gringotts-wizarding-bank'
                    gs_blob_prefix = 'ec76cadf-482d-429d-96e1-461c3350b395/'
                    gs_blob_prefix += '4f8be791-addf-4633-991f-fdeda3bac8c8'
                    gs_file_blob = f'{file_uuid}.{organic_file_name}'
                    gs_blob_name = f'{gs_blob_prefix}/{gs_file_blob}'
                    gs_file_url = f'gs://{gs_bucket_name}/{gs_blob_name}'

                    fixed_time = 1547691253.07010
                    expires = str(round(fixed_time + 3600))
                    pre_signed_gs = furl(
                        url=f'https://storage.googleapis.com.test.{config.domain_name}',
                        path=f'{gs_bucket_name}/{gs_blob_name}',
                        args={
                            'Expires': expires,
                            'GoogleAccessId': 'SOMEACCESSKEY',
                            'Signature': 'SOMESIGNATURE=',
                            'response-content-disposition': f'attachment; filename={gs_file_blob}',
                        })
                    with mock.patch.object(DRSClient,
                                           'get_object',
                                           return_value=Access(method=AccessMethod.gs,
                                                               url=gs_file_url)):
                        pre_signed_url = mock.Mock()
                        pre_signed_url.generate_signed_url.return_value = pre_signed_gs.url
                        with mock.patch.object(TDRFileDownload,
                                               '_get_blob',
                                               return_value=pre_signed_url):
                            with mock.patch('time.time', new=lambda: fixed_time):

                                response = client.request('GET', azul_url.url, redirect=False)
                                self.assertEqual(200 if fetch else 302, response.status)
                                if fetch:
                                    response = json.loads(response.data)
                                    self.assertUrlEqual(pre_signed_gs.url, response['Location'])
                                    self.assertEqual(302, response["Status"])
                                else:
                                    response = dict(response.headers)
                                    self.assertUrlEqual(pre_signed_gs.url, response['Location'])


class TestDSSRepositoryProxy(RepositoryPluginTestCase, DSSUnitTestCase):

    @mock.patch.dict(os.environ,
                     AWS_ACCESS_KEY_ID=mock_access_key_id,
                     AWS_SECRET_ACCESS_KEY=mock_secret_access_key,
                     AWS_SESSION_TOKEN=mock_session_token)
    @mock.patch.object(type(config), 'dss_direct_access_role')
    @mock_s3
    def test_repository_files_proxy(self, dss_direct_access_role):
        dss_direct_access_role.return_value = None
        self.maxDiff = None
        key = ("blobs/6929799f227ae5f0b3e0167a6cf2bd683db097848af6ccde6329185212598779"
               ".f2237ad0a776fd7057eb3d3498114c85e2f521d7"
               ".7e892bf8f6aa489ccb08a995c7f017e1."
               "847325b6")
        bucket_name = 'org-humancellatlas-dss-checkout-staging'
        s3 = aws.client('s3')
        s3.create_bucket(Bucket=bucket_name)
        s3.upload_fileobj(Bucket=bucket_name, Fileobj=io.BytesIO(b'foo'), Key=key)
        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T121154.054628Z'
        organic_file_name = 'foo.txt'
        file_doc = {
            'name': organic_file_name,
            'version': file_version,
            'drs_path': None,
            'size': 3,
        }
        with mock.patch.object(IndexQueryService, 'get_data_file', return_value=file_doc):
            dss_url = furl(
                url=config.dss_endpoint,
                path='/v1/files',
                args={
                    'replica': 'aws',
                    'version': file_version
                }).add(path=file_uuid)
            dss_token = 'some_token'
            dss_url_with_token = dss_url.copy().add(args={'token': dss_token})
            for fetch in True, False:
                for wait in None, 0, 1:
                    for file_name, signature in [(None, 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM='),
                                                 (organic_file_name, 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM=',),
                                                 ('foo bar.txt', 'grbM6udwp0n/QE/L/RYfjtQCS/U='),
                                                 ('foo&bar.txt', 'r4C8YxpJ4nXTZh+agBsfhZ2e7fI=')]:
                        with self.subTest(fetch=fetch, file_name=file_name, wait=wait):
                            with ResponsesHelper() as helper:
                                helper.add_passthru(self.base_url)
                                fixed_time = 1547691253.07010
                                expires = str(round(fixed_time + 3600))
                                s3_url = furl(
                                    url=f'https://{bucket_name}.s3.amazonaws.com',
                                    path=key,
                                    args={
                                        'AWSAccessKeyId': 'SOMEACCESSKEY',
                                        'Signature': 'SOMESIGNATURE=',
                                        'x-amz-security-token': 'SOMETOKEN',
                                        'Expires': expires
                                    })
                                helper.add(responses.Response(method='GET',
                                                              url=dss_url.url,
                                                              status=301,
                                                              headers={'Location': dss_url_with_token.url,
                                                                       'Retry-After': '10'}))
                                azul_url = furl(
                                    url=self.base_url,
                                    path='/fetch/repository/files' if fetch else '/repository/files',
                                    args={
                                        'catalog': self.catalog,
                                        'version': file_version
                                    }).add(path=file_uuid)
                                if wait is not None:
                                    azul_url.args['wait'] = str(wait)
                                if file_name is not None:
                                    azul_url.args['fileName'] = file_name

                                def request_azul(url, expect_status):
                                    retry_after = 1
                                    expect_retry_after = None if wait or expect_status == 302 else retry_after
                                    before = time.monotonic()
                                    with mock.patch.object(type(aws), 'dss_checkout_bucket', return_value=bucket_name):
                                        with mock.patch('time.time', new=lambda: 1547691253.07010):
                                            response = requests.get(url, allow_redirects=False)
                                    if wait and expect_status == 301:
                                        self.assertLessEqual(retry_after, time.monotonic() - before)
                                    if fetch:
                                        self.assertEqual(200, response.status_code)
                                        response = response.json()
                                        self.assertEqual(expect_status, response["Status"])
                                    else:
                                        if response.status_code != expect_status:
                                            response.raise_for_status()
                                        response = dict(response.headers)
                                    if expect_retry_after is None:
                                        self.assertNotIn('Retry-After', response)
                                    else:
                                        actual_retry_after = response['Retry-After']
                                        if fetch:
                                            self.assertEqual(expect_retry_after, actual_retry_after)
                                        else:
                                            self.assertEqual(str(expect_retry_after), actual_retry_after)
                                    return response['Location']

                                location = request_azul(url=azul_url.url, expect_status=301)

                                if file_name is None:
                                    file_name = organic_file_name

                                azul_url.args['token'] = dss_token
                                azul_url.args['requestIndex'] = '1'
                                azul_url.args['fileName'] = file_name
                                azul_url.args['replica'] = 'aws'
                                self.assertUrlEqual(azul_url, location)

                                helper.add(responses.Response(method='GET',
                                                              url=dss_url_with_token.url,
                                                              status=302,
                                                              headers={'Location': s3_url.url}))

                                location = request_azul(url=location, expect_status=302)

                                re_pre_signed_s3_url = furl(
                                    url=f'https://{bucket_name}.s3.amazonaws.com',
                                    path=key,
                                    args={
                                        'response-content-disposition': f'attachment;filename={file_name}',
                                        'AWSAccessKeyId': mock_access_key_id,
                                        'Signature': signature,
                                        'Expires': expires,
                                        'x-amz-security-token': mock_session_token
                                    })
                                self.assertUrlEqual(re_pre_signed_s3_url, location)
