import base64
import binascii
import json
import logging
from typing import (
    Optional,
    Tuple,
    Union,
)
import uuid

from botocore.exceptions import (
    ClientError,
)
from furl import (
    furl,
)

from azul import (
    CatalogName,
)
from azul.service import (
    AbstractService,
    Filters,
)
from azul.service.manifest_service import (
    Manifest,
    ManifestFormat,
)
from azul.service.step_function_helper import (
    StateMachineError,
    StepFunctionHelper,
)
from azul.types import (
    JSON,
    MutableJSON,
)

logger = logging.getLogger(__name__)


class AsyncManifestService(AbstractService):
    """
    Starting and checking the status of manifest generation jobs.
    """
    step_function_helper = StepFunctionHelper()

    def __init__(self, state_machine_name):
        self.state_machine_name = state_machine_name

    @classmethod
    def encode_token(cls, params: JSON) -> str:
        return base64.urlsafe_b64encode(json.dumps(params).encode()).decode()

    @classmethod
    def decode_token(cls, token: str) -> MutableJSON:
        try:
            token = json.loads(base64.urlsafe_b64decode(token).decode())
            if 'execution_id' not in token:
                raise KeyError
        except (KeyError, UnicodeDecodeError, binascii.Error, json.decoder.JSONDecodeError):
            raise ValueError('Invalid token given')
        else:
            return token

    def start_or_inspect_manifest_generation(self,
                                             self_url,
                                             format_: ManifestFormat,
                                             catalog: CatalogName,
                                             filters: Filters,
                                             token: Optional[str] = None,
                                             object_key: Optional[str] = None
                                             ) -> Tuple[int, Manifest]:
        """
        If token is None, start a manifest generation process and returns its
        status. Otherwise return the status of the manifest generation process
        represented by the token.

        :raises ValueError: if token is misformatted or invalid

        :raises StateMachineError: if the state machine execution failed

        :return: Tuple of time to wait and a manifest object with a URL to try.
                 0 wait time indicates success, in which case the URL will be a
                 presigned URL to the actual manifest.
        """
        if token is None:
            execution_id = str(uuid.uuid4())
            self._start_manifest_generation(format_, catalog, filters, execution_id, object_key)
            token = {'execution_id': execution_id}
        else:
            token = self.decode_token(token)
        request_index = token.get('request_index', 0)
        time_or_manifest = self._get_manifest_status(token['execution_id'], request_index)
        if isinstance(time_or_manifest, int):
            request_index += 1
            token['request_index'] = request_index
            location = furl(self_url, args={'token': self.encode_token(token)})
            return time_or_manifest, Manifest(location=location.url,
                                              was_cached=False,
                                              properties={})
        elif isinstance(time_or_manifest, Manifest):
            return 0, time_or_manifest
        else:
            assert False

    def _start_manifest_generation(self,
                                   format_: ManifestFormat,
                                   catalog: CatalogName,
                                   filters: Filters,
                                   execution_id: str,
                                   object_key: Optional[str]
                                   ) -> None:
        """
        Start the execution of a state machine generating the manifest

        :param filters: filters to use for the manifest
        :param execution_id: name to give the execution (must be unique across executions of the state machine)
        """
        self.step_function_helper.start_execution(self.state_machine_name,
                                                  execution_id,
                                                  execution_input=dict(format=format_.value,
                                                                       catalog=catalog,
                                                                       filters=filters,
                                                                       object_key=object_key))

    def _get_next_wait_time(self, request_index: int) -> int:
        wait_times = [1, 1, 4, 6, 10]
        return wait_times[min(request_index, len(wait_times) - 1)]

    def _get_manifest_status(self, execution_id: str, request_index: int) -> Union[int, Manifest]:
        """
        Returns either the time to wait or a manifest object with the location
        of the result.
        """
        try:
            execution = self.step_function_helper.describe_execution(self.state_machine_name, execution_id)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise ValueError('Invalid token given')
            else:
                raise
        output = execution.get('output', None)
        status = execution['status']
        if status == 'SUCCEEDED':
            # Because describe_execution is eventually consistent output may
            # not yet be present
            if output is None:
                return 1
            else:
                output = json.loads(output)
                return Manifest.from_json(output)
        elif status == 'RUNNING':
            return self._get_next_wait_time(request_index)
        else:
            raise StateMachineError(status, output)
