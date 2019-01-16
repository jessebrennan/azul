from abc import abstractmethod, ABCMeta
import importlib.util
import os
import sys
import time
import logging
import unittest

import requests
from threading import Thread
# noinspection PyPackageRequirements
from chalice.local import LocalDevServer
# noinspection PyPackageRequirements
from chalice.config import Config as ChaliceConfig

from azul import config


log = logging.getLogger(__name__)


class ChaliceServerThread(Thread):
    def __init__(self, app, config, host, port):
        super().__init__()
        self.server_wrapper = LocalDevServer(app, config, host, port)

    def run(self):
        self.server_wrapper.serve_forever()

    def kill_thread(self):
        self.server_wrapper.server.shutdown()
        self.server_wrapper.server.server_close()

    @property
    def address(self):
        return self.server_wrapper.server.server_address


class LocalAppTestCase(unittest.TestCase, metaclass=ABCMeta):
    """
    A mixin for test cases against a locally running instance of a AWS Lambda Function aka Chalice application. By
    default, the local instance will use the remote AWS Elasticsearch domain configured via AZUL_ES_DOMAIN or
    AZUL_ES_ENDPOINT. To use a locally running ES instance, combine this mixin with ElasticsearchTestCase. Be sure to
    list ElasticsearchTestCase first such that this mixin picks up the environment overrides made by
    ElasticsearchTestCase.
    """

    @classmethod
    @abstractmethod
    def lambda_name(cls) -> str:
        """
        Return the name of the AWS Lambda function aka. Chalice app to start locally. Must match the name of a
        subdirectory of $AZUL_HOME/lambdas. Subclasses must override this to select which Chalice app to start locally.
        """
        raise NotImplementedError()

    @property
    def base_url(self):
        """
        The HTTP endpoint of the locally running Chalice application. Subclasses should use this to derive the URLs
        for the test requests that they issue.
        """
        host, port = self.server_thread.address
        return f"http://{host}:{port}"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Load the application module without modifying `sys.path` and without adding it to `sys.modules`. This
        # simplifies tear down and isolates the app modules from different lambdas loaded by different concrete
        # subclasses. It does, however, violate this one invariant: `sys.modules[module.__name__] == module`
        path = os.path.join(config.project_root, 'lambdas', cls.lambda_name(), 'app.py')
        spec = importlib.util.spec_from_file_location('__main__', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert path == module.__file__
        assert module.__name__ == '__main__'
        cls.app_module = module

    @classmethod
    def tearDownClass(cls):
        cls.app_module = None
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        log.debug("Setting up tests")
        log.debug("Created Thread")
        self.server_thread = ChaliceServerThread(self.app_module.app, self.chalice_config(), 'localhost', 0)
        log.debug("Started Thread")
        self.server_thread.start()
        deadline = time.time() + 10
        while True:
            url = self.base_url
            try:
                response = requests.get(url)
                response.raise_for_status()
            except Exception:
                if time.time() > deadline:
                    raise
                log.debug("Unable to connect to server", exc_info=True)
                time.sleep(1)
            else:
                break

    def chalice_config(self):
        return ChaliceConfig()

    def tearDown(self):
        log.debug("Tearing Down Data")
        self.server_thread.kill_thread()
        self.server_thread.join(timeout=10)
        if self.server_thread.is_alive():
            self.fail('Thread is still alive after joining')
