import asyncio
import logging
import os
import pathlib
import platform
import sys
import time
from typing import Dict

import docker
import pytest
from aiodistbus import EntryPoint, EventBus

import chimerapy.engine as cpe
from chimerapy.engine.networking.publisher import Publisher

from .mock import DockeredWorker

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = TEST_DIR / "data"
TEST_SAMPLE_DATA_DIR = TEST_DIR / "mock" / "data"

# Try to get Github Actions environment variable
try:
    current_platform = os.environ["MANUAL_OS_SET"]
    running_on_github_actions = os.environ["RUNNING_ON_GA"]
except Exception:
    current_platform = platform.system()
    running_on_github_actions = "Native"

# Skip marks
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
    "IocpeProactor",
]


def pytest_configure():
    for logger_name in disable_loggers:
        logger = logging.getLogger(logger_name)
        logger.disabled = True
        logger.propagate = False


@pytest.fixture(scope="session")
def event_loop():

    if sys.platform in ["win32", "cygwin", "cli"]:
        import winloop

        winloop.install()
    else:
        import uvloop

        uvloop.install()
    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def bus():
    bus = EventBus(debug=True)
    yield bus
    await bus.close()


@pytest.fixture
async def entrypoint(bus):
    entrypoint = EntryPoint()
    await entrypoint.connect(bus)
    yield entrypoint
    await entrypoint.close()


@pytest.fixture
def pub():
    pub = Publisher()
    pub.start()
    yield pub
    pub.shutdown()


@pytest.fixture
def logreceiver():
    listener = cpe._logger.get_node_id_zmq_listener()
    listener.start()
    yield listener
    listener.stop()
    listener.join()


@pytest.fixture(autouse=True)
def slow_interval_between_tests():
    yield
    time.sleep(0.1)


@pytest.fixture
async def manager():
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    await manager.aserve()
    yield manager
    await manager.async_shutdown()


@pytest.fixture
async def worker():
    worker = cpe.Worker(name="local", id="local", port=0)
    await worker.aserve()
    yield worker
    await worker.async_shutdown()


@pytest.fixture(autouse=True)
def disable_file_logging():
    cpe.config.set("manager.logs-sink.enabled", False)


class GenNode(cpe.Node):
    def setup(self):
        self.value = 2

    def step(self):
        time.sleep(0.5)
        self.logger.debug(self.value)
        return self.value


class ConsumeNode(cpe.Node):
    def setup(self):
        self.coef = 3

    def step(self, data_chunks: Dict[str, cpe.DataChunk]):
        time.sleep(0.1)
        # Extract the data
        self.logger.debug(f"{self}: {data_chunks}")
        # self.logger.debug(
        #     f"{self}: inside step, with {data_chunks} - {data_chunks['Gen1']}"
        # )
        value = data_chunks["Gen1"].get("default")["value"]
        output = self.coef * value
        return output


class SlowSetupNode(cpe.Node):
    def setup(self):
        time.sleep(2)
        self.value = 5

    def step(self):
        time.sleep(0.5)
        # self.logger.debug(self.value)
        return self.value


@pytest.fixture
def gen_node():
    return GenNode(name="Gen1")


@pytest.fixture
def con_node():
    return ConsumeNode(name="Con1")


@pytest.fixture
def slow_node():
    return SlowSetupNode(name="Slo1")


@pytest.fixture
def graph(gen_node, con_node):

    # Define graph
    _graph = cpe.Graph()
    _graph.add_nodes_from([gen_node, con_node])
    _graph.add_edge(src=gen_node, dst=con_node)

    return _graph
