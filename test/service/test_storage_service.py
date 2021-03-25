import json
import tempfile
from unittest.mock import (
    patch,
)

from moto import (
    mock_s3,
    mock_sts,
)
import requests

from azul.logging import (
    configure_test_logging,
)
from azul.service.storage_service import (
    MultipartUploadError,
    MultipartUploadHandler,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from service import (
    StorageServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class StorageServiceTest(AzulUnitTestCase, StorageServiceTestCase):
    """
    Functional Test for Storage Service
    """

    @mock_s3
    @mock_sts
    def test_upload_tags(self):
        self.storage_service.create_bucket()

        object_key = 'test_file'
        with tempfile.NamedTemporaryFile('w') as f:
            f.write('some contents')
            for tags in (None, {}, {'Name': 'foo', 'game': 'bar'}):
                with self.subTest(tags=tags):
                    self.storage_service.upload(file_path=f.name,
                                                object_key=object_key,
                                                tagging=tags)
                    if tags is None:
                        tags = {}
                    self.assertEqual(tags,
                                     self.storage_service.get_object_tagging(object_key))

    @mock_s3
    @mock_sts
    def test_simple_get_put(self):
        sample_key = 'foo-simple'
        sample_content = b'bar'

        self.storage_service.create_bucket()

        # NOTE: Ensure that the key does not exist before writing.
        with self.assertRaises(self.storage_service.client.exceptions.NoSuchKey):
            self.storage_service.get(sample_key)

        self.storage_service.put(sample_key, sample_content)

        self.assertEqual(sample_content, self.storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_simple_get_unknown_item(self):
        sample_key = 'foo-simple'

        self.storage_service.create_bucket()

        with self.assertRaises(self.storage_service.client.exceptions.NoSuchKey):
            self.storage_service.get(sample_key)

    @mock_s3
    @mock_sts
    def test_presigned_url(self):
        sample_key = 'foo-presigned-url'
        sample_content = json.dumps({"a": 1})

        self.storage_service.create_bucket()
        self.storage_service.put(sample_key, sample_content.encode())

        for file_name in None, 'foo.json':
            with self.subTest(file_name=file_name):
                presigned_url = self.storage_service.get_presigned_url(sample_key,
                                                                       file_name=file_name)
                response = requests.get(presigned_url)
                if file_name is None:
                    self.assertNotIn('Content-Disposition', response.headers)
                else:
                    # noinspection PyUnreachableCode
                    if False:  # no coverage
                        # Unfortunately, moto does not support emulating S3's mechanism of specifying response headers
                        # via request parameters (https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html,
                        # section Request Parameters).
                        self.assertEqual(response.headers['Content-Disposition'], f'attachment;filename="{file_name}"')
                self.assertEqual(sample_content, response.text)

    @mock_s3
    @mock_sts
    def test_multipart_upload_ok_with_one_part(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            b'a' * 1024  # The last part can be smaller than the limit.
        ]
        expected_content = b"".join(sample_content_parts)

        self.storage_service.create_bucket()
        with MultipartUploadHandler(sample_key) as upload:
            for part in sample_content_parts:
                upload.push(part)

        self.assertEqual(expected_content, self.storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_multipart_upload_ok_with_n_parts(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            b'a' * 5242880,  # The minimum file size for multipart upload is 5 MB.
            b'b' * 5242880,
            b'c' * 1024  # The last part can be smaller than the limit.
        ]
        expected_content = b''.join(sample_content_parts)

        self.storage_service.create_bucket()
        with MultipartUploadHandler(sample_key) as upload:
            for part in sample_content_parts:
                upload.push(part)

        self.assertEqual(expected_content,
                         self.storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_with_out_of_bound_part(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            b'a' * 1024,  # This part will cause an error raised by MPU.
            b'b' * 5242880,
            b'c' * 1024
        ]

        self.storage_service.create_bucket()

        with self.assertRaises(MultipartUploadError):
            with MultipartUploadHandler(sample_key) as upload:
                for part in sample_content_parts:
                    upload.push(part)

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_inside_context_with_nothing_pushed(self):
        sample_key = 'foo-multipart-upload-error'
        sample_content_parts = [
            b'a' * 5242880,
            b'b' * 5242880,
            1234567,  # This should cause an error.
            b'c' * 1024
        ]

        self.storage_service.create_bucket()
        with self.assertRaises(MultipartUploadError):
            with MultipartUploadHandler(sample_key) as upload:
                for part in sample_content_parts:
                    upload.push(part)

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_inside_thread_with_nothing_pushed(self):
        sample_key = 'foo-multipart-upload-error'
        sample_content_parts = [
            b'a' * 5242880,
            b'b' * 5242880
        ]

        self.storage_service.create_bucket()
        with patch.object(MultipartUploadHandler, '_upload_part', side_effect=RuntimeError('test')):
            with self.assertRaises(MultipartUploadError):
                with MultipartUploadHandler(sample_key) as upload:
                    for part in sample_content_parts:
                        upload.push(part)
