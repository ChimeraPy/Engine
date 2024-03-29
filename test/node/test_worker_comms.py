from typing import Dict

import pytest
from aiohttp import web

import chimerapy.engine as cpe
from chimerapy.engine.data_protocols import NodeDiagnostics, NodePubTable
from chimerapy.engine.eventbus import EventBus
from chimerapy.engine.networking.enums import NODE_MESSAGE, WORKER_MESSAGE
from chimerapy.engine.networking.server import Server
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine.node.worker_comms_service import WorkerCommsService
from chimerapy.engine.states import NodeState

logger = cpe._logger.getLogger("chimerapy-engine")


class MockWorker:
    def __init__(self):
        self.node_states: Dict[str, NodeState] = {}
        self.server = Server(
            id="test_server",
            port=0,
            ws_handlers={
                NODE_MESSAGE.STATUS: self.node_status_update,
                NODE_MESSAGE.REPORT_GATHER: self.node_report_gather,
                NODE_MESSAGE.REPORT_RESULTS: self.node_report_results,
                NODE_MESSAGE.DIAGNOSTICS: self.node_diagnostics,
            },
        )

    async def setup(self):
        await self.server.async_serve()

    async def node_status_update(self, msg: Dict, ws: web.WebSocketResponse):

        # self.logger.debug(f"{self}: note_status_update: ", msg)
        node_state = NodeState.from_dict(msg["data"])
        node_id = node_state.id

        # Update our records by grabbing all data from the msg
        self.node_states[node_id] = node_state

    async def node_report_gather(self, msg: Dict, ws: web.WebSocketResponse):
        ...

    async def node_report_results(self, msg: Dict, ws: web.WebSocketResponse):
        ...

    async def node_diagnostics(self, msg: Dict, ws: web.WebSocketResponse):
        ...

    async def async_shutdown(self):
        await self.server.async_shutdown()


@pytest.fixture
async def mock_worker():
    mock_worker = MockWorker()
    await mock_worker.setup()
    yield mock_worker
    await mock_worker.async_shutdown()


@pytest.fixture
async def worker_comms_setup(mock_worker):

    # Event Loop
    eventbus = EventBus()

    # Create sample state
    state = NodeState(id="test_worker_comms")
    node_config = NodeConfig()

    # Create the service
    worker_comms = WorkerCommsService(
        "worker_comms",
        host=mock_worker.server.host,
        port=mock_worker.server.port,
        node_config=node_config,
        state=state,
        eventbus=eventbus,
        logger=logger,
    )

    yield (worker_comms, mock_worker.server)
    await mock_worker.async_shutdown()


def test_instanticate(worker_comms_setup):
    ...


@pytest.mark.parametrize(
    "method_name, method_params",
    [
        ("start_node", {}),
        ("record_node", {}),
        ("stop_node", {}),
        ("provide_collect", {}),
        ("execute_registered_method", {"data": {"method_name": "", "params": {}}}),
        ("process_node_pub_table", {"data": NodePubTable().to_dict()}),
        ("async_step", {}),
        ("provide_gather", {}),
        ("send_diagnostics", NodeDiagnostics()),
        ("enable_diagnostics", {"data": {"enable": True}}),
    ],
)
async def test_methods(worker_comms_setup, method_name, method_params):
    worker_comms, _ = worker_comms_setup

    # Start the server
    await worker_comms.setup()

    # Run method
    method = getattr(worker_comms, method_name)
    await method(method_params)

    # Shutdown
    await worker_comms.teardown()


@pytest.mark.parametrize(
    "signal, data",
    [
        (WORKER_MESSAGE.BROADCAST_NODE_SERVER, NodePubTable().to_dict()),
        (WORKER_MESSAGE.REQUEST_STEP, {}),
        (WORKER_MESSAGE.REQUEST_COLLECT, {}),
        (WORKER_MESSAGE.REQUEST_GATHER, {}),
        (WORKER_MESSAGE.START_NODES, {}),
        (WORKER_MESSAGE.RECORD_NODES, {}),
        (WORKER_MESSAGE.STOP_NODES, {}),
        (WORKER_MESSAGE.REQUEST_METHOD, {"method_name": "", "params": {}}),
        (WORKER_MESSAGE.DIAGNOSTICS, {"enable": False}),
    ],
)
async def test_ws_signals(worker_comms_setup, signal, data):
    worker_comms, server = worker_comms_setup

    # Start the server
    await worker_comms.setup()

    # Run method
    await server.async_send(
        client_id=worker_comms.state.id, signal=signal, data=data, ok=True
    )

    # Shutdown
    await worker_comms.teardown()
