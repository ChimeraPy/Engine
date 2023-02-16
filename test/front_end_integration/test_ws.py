from typing import Dict
import time

import pytest
from aiohttp import web
import chimerapy as cp
from chimerapy.networking.enums import MANAGER_MESSAGE
from chimerapy.states import ManagerState

from ..conftest import GenNode

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
    client = cp.Client(
        id="test_ws",
        host=manager.host,
        port=manager.port,
        ws_handlers={MANAGER_MESSAGE.NODE_STATUS_UPDATE: record.node_update_counter},
    )
    client.connect()

    yield client, record
    client.shutdown()


# @pytest.mark.skip()
def test_node_updates(test_ws_client, manager, worker):

    client, record = test_ws_client

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name=f"Gen1")
    simple_graph.add_nodes_from([new_node])
    mapping = {worker.id: [new_node.id]}

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)
    manager.commit_graph(simple_graph, mapping)

    time.sleep(5)

    # New test assertations
    assert record.network_state == manager.state
