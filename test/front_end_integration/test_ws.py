from typing import Dict

from aiohttp import web
import chimerapy as cp
from chimerapy.networking.enums import MANAGER_MESSAGE

from ..conftest import GenNode

logger = cp._logger.getLogger("chimerapy")


def test_node_updates(manager, worker):

    msg_counter = 0

    async def node_update_counter(msg: Dict, ws: web.WebSocketResponse):
        logger.debug(msg)
        msg_counter += 1

    # Simulating a front-end client with a Python WS client
    client = cp.Client(
        id="test_ws",
        host=manager.host,
        port=manager.port,
        ws_handler={MANAGER_MESSAGE.REPORT_STATUS: node_update_counter},
    )
    client.connect()

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name=f"Gen1")
    simple_graph.add_nodes_from([new_node])
    mapping = {worker.id: [new_node.id]}

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(mapping)

    # Request node creation
    assert manager.request_node_creation(worker_id=worker.id, node_id=new_node.id)
    assert new_node.id in manager.workers[worker.id]["nodes_status"]
