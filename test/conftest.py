from typing import Dict, Any
import time
import logging
import platform

logger = logging.getLogger("chimerapy")

import docker
import pytest

from chimerapy import Manager, Worker, Graph, Node
from .mock import DockeredWorker


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
    if platform.system() != "Linux":
        pytest.skip(reason="This fixture should only be created when used in Linux")
        return None
    else:
        c = docker.DockerClient(base_url="unix://var/run/docker.sock")
        return c


@pytest.fixture
def dockered_worker(docker_client):
    if platform.system() != "Linux":
        pytest.skip(reason="This fixture should only be created when used in Linux")
        return None
    else:
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
