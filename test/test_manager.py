import os
from pathlib import Path

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp
from chimerapy import Manager

from .conftest import GenNode, ConsumeNode

# Constants
TEST_DIR = Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = TEST_DIR / "data"


@pytest.fixture(scope="module")
def testbed_setup():

    # Create Manager
    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cp.Worker(name="local", port=0)
    gen_node = GenNode(name="Gen")
    con_node = ConsumeNode(name="Con")

    # Define graph
    simple_graph = cp.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    # Register graph
    manager._register_graph(simple_graph)

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    yield (manager, worker, gen_node, con_node)

    manager.shutdown()
    worker.shutdown()


def test_manager_logdir_string():
    manager = Manager(logdir=".", port=0)
    assert manager.logdir.parent == Path(".").resolve()
    manager.shutdown()


def test_manager_logdir_path():
    manager = Manager(logdir=Path("."), port=0)
    assert manager.logdir.parent == Path(".").resolve()
    manager.shutdown()


def test_manager_creating_node(testbed_setup):

    manager, worker, gen_node, con_node = testbed_setup

    # Create Node
    future = manager._request_node_creation(worker_id=worker.id, node_id=gen_node.id)
    assert future.result(timeout=30)
    assert gen_node.id in manager.state.workers[worker.id].nodes

    # Destroy Node
    future = manager._request_node_destruction(worker_id=worker.id, node_id=gen_node.id)
    assert future.result(timeout=10)
    assert gen_node.id not in manager.state.workers[worker.id].nodes


def test_manager_create_connections(testbed_setup):

    manager, worker, gen_node, con_node = testbed_setup

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

    # Create connections
    connection_future = manager._request_connection_creation(worker_id=worker.id)
    assert connection_future.result(timeout=30)
