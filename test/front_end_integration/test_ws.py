from ..conftest import GenNode

import time
from typing import Dict

import pytest
import chimerapy as cp
from chimerapy.networking import Client
from chimerapy.networking.enums import MANAGER_MESSAGE
from chimerapy.states import ManagerState


logger = cp._logger.getLogger("chimerapy")


class Record:
    def __init__(self):
        self.network_state = None

    async def node_update_counter(self, msg: Dict):
        self.network_state = ManagerState.from_dict(msg["data"])


@pytest.fixture
def test_ws_client(manager):

    # Create a record
    record = Record()

    # Simulating a front-end client with a Python WS client
    client = Client(
        id="test_ws",
        host=manager.host,
        port=manager.port,
        ws_handlers={
            MANAGER_MESSAGE.NODE_STATUS_UPDATE: record.node_update_counter,
            MANAGER_MESSAGE.NETWORK_STATUS_UPDATE: record.node_update_counter,
        },
    )
    client.connect()

    yield client, record
    client.shutdown()


def test_node_updates(test_ws_client, manager, worker):
    client, record = test_ws_client

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name="Gen1")
    simple_graph.add_nodes_from([new_node])
    mapping = {worker.id: [new_node.id]}

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)
    manager.commit_graph(simple_graph, mapping).result(timeout=30)
    time.sleep(3)
    assert record.network_state == manager.state


def test_worker_network_updates(test_ws_client, manager, worker):
    client, record = test_ws_client

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)
    time.sleep(3)
    assert record.network_state == manager.state

    worker.deregister()
    time.sleep(3)
    assert record.network_state == manager.state


def test_node_creation_and_destruction_network_updates(test_ws_client, manager, worker):
    client, record = test_ws_client

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name="Gen1", id="Gen1")
    simple_graph.add_nodes_from([new_node])

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)
    manager._register_graph(simple_graph)

    # Test construction
    manager._request_node_creation(worker_id=worker.id, node_id="Gen1").result(
        timeout=30
    )
    time.sleep(2)
    assert record.network_state == manager.state

    # Test destruction
    manager._request_node_destruction(worker_id=worker.id, node_id="Gen1").result(
        timeout=10
    )
    time.sleep(2)
    assert record.network_state == manager.state


def test_reset_network_updates(test_ws_client, manager, worker):
    client, record = test_ws_client

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name="Gen1")
    simple_graph.add_nodes_from([new_node])
    mapping = {worker.id: [new_node.id]}

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)
    manager.commit_graph(simple_graph, mapping).result(timeout=30)
    time.sleep(3)
    assert record.network_state == manager.state

    # Reset
    manager.reset(keep_workers=True)
    time.sleep(3)
    assert record.network_state == manager.state

    # Recommit graph
    manager.commit_graph(simple_graph, mapping).result(timeout=30)
    time.sleep(3)
    assert record.network_state == manager.state
