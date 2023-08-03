from .mock import DockeredWorker

import time
import logging
import pathlib
import os
import platform
from typing import Dict

import docker
import pytest

import chimerapy.engine as cpe

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
def manager():
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    yield manager
    manager.shutdown()


@pytest.fixture
def worker():
    worker = cpe.Worker(name="local", id="local", port=0)
    yield worker
    worker.shutdown()


@pytest.fixture
def docker_client():
    logger.info(f"DOCKER CLIENT: {current_platform}")
    c = docker.DockerClient(base_url="unix://var/run/docker.sock")
    return c


@pytest.fixture(autouse=True)
def disable_file_logging():
    cpe.config.set("manager.logs-sink.enabled", False)


@pytest.fixture
def dockered_worker(docker_client):
    logger.info(f"DOCKER WORKER: {current_platform}")
    dockered_worker = DockeredWorker(docker_client, name="test")
    yield dockered_worker
    dockered_worker.shutdown()


class LowFrequencyNode(cpe.Node):
    def setup(self):
        self.i = 0

    def step(self):
        data_chunk = cpe.DataChunk()
        if self.i == 0:
            time.sleep(0.5)
            self.i += 1
            data_chunk.add("i", self.i)
            return data_chunk
        else:
            time.sleep(3)
            self.i += 1
            data_chunk.add("i", self.i)
            return data_chunk


class HighFrequencyNode(cpe.Node):
    def setup(self):
        self.i = 0

    def step(self):
        time.sleep(0.1)
        self.i += 1
        data_chunk = cpe.DataChunk()
        data_chunk.add("i", self.i)
        return data_chunk


class SubsequentNode(cpe.Node):
    def setup(self):
        self.record = {}

    def step(self, data: Dict[str, cpe.DataChunk]):

        for k, v in data.items():
            self.record[k] = v

        data_chunk = cpe.DataChunk()
        data_chunk.add("record", self.record)

        return data_chunk


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


@pytest.fixture
def single_node_no_connections_manager(manager, worker, gen_node):

    # Define graph
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node])

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        simple_graph,
        {
            worker.id: [gen_node.id],
        },
    ).result(timeout=30)

    return manager


@pytest.fixture
def multiple_nodes_one_worker_manager(manager, worker, gen_node, con_node):

    # Define graph
    graph = cpe.Graph()
    graph.add_nodes_from([gen_node, con_node])
    graph.add_edge(gen_node, con_node)

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        graph,
        {
            worker.id: [gen_node.id, con_node.id],
        },
    ).result(timeout=30)

    return manager


@pytest.fixture
def multiple_nodes_multiple_workers_manager(manager, gen_node, con_node):

    # Define graph
    graph = cpe.Graph()
    graph.add_nodes_from([gen_node, con_node])
    graph.add_edge(gen_node, con_node)

    worker1 = cpe.Worker(name="local", port=0)
    worker2 = cpe.Worker(name="local2", port=0)

    worker1.connect(method="ip", host=manager.host, port=manager.port)
    worker2.connect(method="ip", host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        graph, {worker1.id: [gen_node.id], worker2.id: [con_node.id]}
    ).result(timeout=60)

    yield manager

    worker1.shutdown()
    worker2.shutdown()


@pytest.fixture
def slow_single_node_single_worker_manager(manager, worker, slow_node):

    # Define graph
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([slow_node])

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        simple_graph,
        {
            worker.id: [slow_node.id],
        },
    ).result(timeout=30)

    return manager


@pytest.fixture
def dockered_single_node_no_connections_manager(dockered_worker, manager, gen_node):

    # Define graph
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node])

    # Connect to the manager
    dockered_worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        simple_graph,
        {
            dockered_worker.id: [gen_node.id],
        },
    ).result(timeout=30)

    return manager


@pytest.fixture
def dockered_multiple_nodes_one_worker_manager(
    dockered_worker, manager, gen_node, con_node
):

    # Define graph
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(gen_node, con_node)

    # Connect to the manager
    dockered_worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        simple_graph,
        {
            dockered_worker.id: [gen_node.id, con_node.id],
        },
    ).result(timeout=30)

    return manager


@pytest.fixture
def dockered_multiple_nodes_multiple_workers_manager(
    docker_client, manager, gen_node, con_node
):

    # Define graph
    graph = cpe.Graph()
    graph.add_nodes_from([gen_node, con_node])
    graph.add_edge(gen_node, con_node)

    worker1 = DockeredWorker(docker_client, name="local")
    worker2 = DockeredWorker(docker_client, name="local2")

    worker1.connect(host=manager.host, port=manager.port)
    worker2.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(
        graph, {worker1.id: [gen_node.id], worker2.id: [con_node.id]}
    ).result(timeout=30)

    yield manager

    worker1.shutdown()
    worker2.shutdown()
