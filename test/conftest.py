from typing import Dict, Any
import time
import logging
import pathlib
import os
import platform
import socket
import pickle
import threading
import queue

import docker
import pytest

import chimerapy as cp
from .mock import DockeredWorker

logger = cp._logger.getLogger("chimerapy")

# Constants
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = TEST_DIR / "data"

# Try to get Github Actions environment variable
try:
    current_platform = os.environ["MANUAL_OS_SET"]
    running_on_github_actions = os.environ["RUNNING_ON_GA"]
except:
    current_platform = platform.system()
    running_on_github_actions = "Native"

# Skip marks
linux_run_only = pytest.mark.skipif(
    current_platform != "Linux", reason="Test only can run on Linux"
)
linux_expected_only = pytest.mark.skipif(
    current_platform != "Linux", reason="Test expected to only pass on Linux"
)
not_github_actions = pytest.mark.skipif(
    running_on_github_actions == "GA",
    reason="Certain test require hardware to execute that is not available in GA",
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
        logger = cp._logger.getLogger("chimerapy")

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
    manager = cp.Manager(logdir=TEST_DATA_DIR)
    yield manager
    manager.shutdown()


@pytest.fixture
def worker():
    worker = cp.Worker(name="local")
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


class GenNode(cp.Node):
    def prep(self):
        self.value = 2

    def step(self):
        time.sleep(0.5)
        logger.debug(self.value)
        return self.value


class ConsumeNode(cp.Node):
    def prep(self):
        self.coef = 3

    def step(self, data: Dict[str, Any]):
        time.sleep(0.1)
        output = self.coef * data["Gen1"]
        logger.debug(output)
        return output


class SlowPrepNode(cp.Node):
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


@pytest.fixture
def save_handler_and_queue():

    save_queue = queue.Queue()
    save_handler = cp.SaveHandler(logdir=TEST_DATA_DIR, save_queue=save_queue)
    save_handler.start()

    return (save_handler, save_queue)
