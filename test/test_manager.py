import os
import time
from pathlib import Path

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp
from chimerapy.manager import Manager
from .conftest import TEST_DATA_DIR

from .conftest import GenNode, ConsumeNode, linux_run_only

# Constants
TEST_DIR = Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = TEST_DIR / "data"


@pytest.fixture(scope="module")
def testbed_setup():

    # Create Manager
    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cp.Worker(name="local", port=0)
    gen_node = GenNode(name="Gen1")
    con_node = ConsumeNode(name="Con1")

    # Define graph
    simple_graph = cp.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    yield (manager, worker, simple_graph, gen_node, con_node)

    manager.shutdown()
    worker.shutdown()


def test_manager_logdir_string():
    manager = Manager(logdir=str(TEST_DATA_DIR), port=0)
    assert manager.logdir.parent == TEST_DATA_DIR
    manager.shutdown()


def test_manager_logdir_path():
    manager = Manager(logdir=TEST_DATA_DIR, port=0)
    assert manager.logdir.parent == TEST_DATA_DIR
    manager.shutdown()


def test_manager_creating_node(testbed_setup):

    manager, worker, graph, gen_node, con_node = testbed_setup
    manager._register_graph(graph)

    # Create Node
    future = manager._request_node_creation(worker_id=worker.id, node_id=gen_node.id)
    assert future.result(timeout=30)
    assert gen_node.id in manager.state.workers[worker.id].nodes

    # Destroy Node
    future = manager.reset()
    assert future.result(timeout=30)


def test_manager_create_connections(testbed_setup):

    manager, worker, graph, gen_node, con_node = testbed_setup
    manager._register_graph(graph)

    # Create Nodes
    gen_future = manager._request_node_creation(
        worker_id=worker.id, node_id=gen_node.id
    )
    con_future = manager._request_node_creation(
        worker_id=worker.id, node_id=con_node.id
    )
    assert gen_future.result(timeout=30) and con_future.result(timeout=30)

    # Get the node information
    get_future = manager._request_node_server_data(worker_id=worker.id)
    assert get_future.result(timeout=10)
    assert manager.nodes_server_table != {}

    # Create connections
    connection_future = manager._request_connection_creation(worker_id=worker.id)
    assert connection_future.result(timeout=30)

    # Teardown
    future = manager.reset()
    assert future.result(timeout=30)


def test_manager_lifecycle_graph(testbed_setup):

    manager, worker, graph, gen_node, con_node = testbed_setup

    future = manager.commit_graph(
        graph=graph, mapping={worker.id: [gen_node.id, con_node.id]}
    )
    assert future.result(timeout=30)
    assert manager.start().result()

    time.sleep(5)

    assert manager.stop().result()
    assert manager.collect().result()

    future = manager.reset(keep_workers=True)
    assert future.result(timeout=30)
