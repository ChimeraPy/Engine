from typing import Dict

import pytest
from aiohttp import web

import chimerapy.engine as cpe
from chimerapy.engine.node.worker_comms_service import WorkerCommsService
from chimerapy.engine.networking.enums import NODE_MESSAGE
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus
from chimerapy.engine.networking.server import Server
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.data_protocols import NodePubTable


logger = cpe._logger.getLogger("chimerapy-engine")


async def node_status_update(msg: Dict, ws: web.WebSocketResponse):
    ...


async def node_report_gather(msg: Dict, ws: web.WebSocketResponse):
    ...


async def node_report_results(msg: Dict, ws: web.WebSocketResponse):
    ...


@pytest.fixture
def server():
    server = Server(
        id="test_server",
        port=0,
        ws_handlers={
            NODE_MESSAGE.STATUS: node_status_update,
            NODE_MESSAGE.REPORT_GATHER: node_report_gather,
            NODE_MESSAGE.REPORT_RESULTS: node_report_results,
        },
    )
    server.serve()
    yield server
    server.shutdown()


@pytest.fixture
def worker_comms_setup(server):

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState(id="test_worker_comms")
    node_config = NodeConfig()

    # Create the service
    worker_comms = WorkerCommsService(
        "worker_comms",
        host=server.host,
        port=server.port,
        node_config=node_config,
        state=state,
        eventbus=eventbus,
        logger=logger,
    )

    return (worker_comms, server)


def test_instanticate(worker_comms_setup):
    ...


@pytest.mark.asyncio
async def test_setup(worker_comms_setup):
    worker_comms, server = worker_comms_setup

    # Start the server
    await worker_comms.setup()
    assert "test_worker_comms" in server.ws_clients
    await worker_comms.teardown()


@pytest.mark.parametrize(
    "method_name, method_params",
    [
        ("start_node", {}),
        ("record_node", {}),
        ("stop_node", {}),
        ("provide_collect", {}),
        ("execute_registered_method", {"data": {"method_name": "", "params": {}}}),
        ("process_node_pub_table", {"data": NodePubTable().to_json()}),
        ("async_step", {}),
        ("provide_gather", {}),
    ],
)
@pytest.mark.asyncio
async def test_methods(worker_comms_setup, method_name, method_params):
    worker_comms, _ = worker_comms_setup

    # Start the server
    await worker_comms.setup()

    # Run method
    method = getattr(worker_comms, method_name)
    await method(method_params)

    # Shutdown
    await worker_comms.teardown()
