from typing import Dict, Any
import time
import logging
import sys
import os
import platform
import socket
import pickle
import threading

import docker
import pytest

from chimerapy import Manager, Worker, Graph, Node
from .mock import DockeredWorker

logger = logging.getLogger("chimerapy")

# Try to get Github Actions environment variable
try:
    current_platform = os.environ["MANUAL_OS_SET"]
except:
    current_platform = platform.system()

linux_run_only = pytest.mark.skipif(
    current_platform != "Linux", reason="Test only can run on Linux"
)
linux_expected_only = pytest.mark.skipif(
    current_platform != "Linux", reason="Test expected to only pass on Linux"
)

disable_loggers = [
    "matplotlib",
    "chardet.charsetprober",
    "matplotlib.font_manager",
    "PIL.PngImagePlugin",
]


def pytest_configure():
    for logger_name in disable_loggers:
        logger = logging.getLogger(logger_name)
        logger.disabled = True
        logger.propagate = False


@pytest.fixture
def logreceiver():
    def listener():

        # Create server and logger to relay messages
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 5555))
        logger = logging.getLogger("")

        # Continue listening until signaled to stop
        while True:

            # Listen for information
            data = s.recv(4096)
            if data == b"die":
                break

            # Dont forget to skip over the 32-bit length prepended
            logrec = pickle.loads(data[4:])
            rec = logging.LogRecord(
                name=logrec["name"],
                level=logrec["levelno"],
                pathname=logrec["pathname"],
                lineno=logrec["lineno"],
                msg=logrec["msg"],
                args=logrec["args"],
                exc_info=logrec["exc_info"],
                func=logrec["funcName"],
            )
            logger.handle(rec)

        # Safely shutdown socket
        s.close()

    # Start relaying thread
    receiver_thread = threading.Thread(target=listener)
    receiver_thread.start()

    yield

    # Shutting down thread
    t = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t.sendto(b"die", ("127.0.0.1", 5555))
    receiver_thread.join()


@pytest.fixture(autouse=True)
def slow_interval_between_tests():
    yield
    time.sleep(0.1)


@pytest.fixture
def manager():
    manager = Manager()
    yield manager
    manager.shutdown()


@pytest.fixture
def worker():
    worker = Worker(name="local")
    yield worker
    worker.shutdown()


@pytest.fixture
def docker_client():
    logger.info(f"DOCKER CLIENT: {current_platform}")
    c = docker.DockerClient(base_url="unix://var/run/docker.sock")
    return c


@pytest.fixture
def dockered_worker(docker_client):
    logger.info(f"DOCKER WORKER: {current_platform}")
    dockered_worker = DockeredWorker(docker_client, name="test")
    yield dockered_worker
    dockered_worker.shutdown()


class GenNode(Node):
    def prep(self):
        self.value = 2

    def step(self):
        time.sleep(0.5)
        logger.debug(self.value)
        return self.value


class ConsumeNode(Node):
    def prep(self):
        self.coef = 3

    def step(self, data: Dict[str, Any]):
        time.sleep(0.1)
        output = self.coef * data["Gen1"]
        logger.debug(output)
        return output


class SlowPrepNode(Node):
    def prep(self):
        time.sleep(5)
        self.value = 5

    def step(self):
        time.sleep(0.5)
        logger.debug(self.value)
        return self.value


@pytest.fixture
def gen_node():
    return GenNode(name="Gen1")


@pytest.fixture
def con_node():
    return ConsumeNode(name="Con1")


@pytest.fixture
def slow_node():
    return SlowPrepNode(name="Slo1")
