import pickle
import json

import aiohttp

import pytest
from pytest_lazyfixture import lazy_fixture

from chimerapy.engine.worker.http_server_service import HttpServerService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.networking.client import Client
from chimerapy.engine.networking.enums import NODE_MESSAGE
from chimerapy.engine.data_protocols import NodePubTable, NodeDiagnostics
from chimerapy.engine.eventbus import EventBus
from chimerapy.engine.states import WorkerState, NodeState
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine import _logger

from ..conftest import TEST_DATA_DIR


logger = _logger.getLogger("chimerapy-engine-worker")


@pytest.fixture
def pickled_gen_node_config(gen_node):
    return pickle.dumps(NodeConfig(gen_node))


@pytest.fixture(scope="module")
def http_server():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Requirements
    state = WorkerState()

    # Create the services
    http_server = HttpServerService(
        name="http_server", state=state, thread=thread, eventbus=eventbus, logger=logger
    )
    thread.exec(http_server.start()).result(timeout=10)
    return http_server


@pytest.fixture(scope="module")
def ws_client(http_server):

    client = Client(
        host=http_server.ip,
        port=http_server.port,
        id="ws_client",
        ws_handlers={},
        parent_logger=logger,
    )
    client.connect()
    yield client
    client.shutdown()


def test_http_server_instanciate(http_server):
    ...


@pytest.mark.parametrize(
    "route_type, route, payload",
    [
        ("post", "/nodes/create", lazy_fixture("pickled_gen_node_config")),
        ("post", "/nodes/destroy", json.dumps({"id": 0})),
        ("get", "/nodes/pub_table", None),
        ("post", "/nodes/pub_table", NodePubTable().to_json()),
        ("get", "/nodes/gather", None),
        ("post", "/nodes/collect", json.dumps({"path": str(TEST_DATA_DIR)})),
        ("post", "/nodes/step", json.dumps({})),
        ("post", "/nodes/start", json.dumps({})),
        ("post", "/nodes/record", json.dumps({})),
        ("post", "/nodes/diagnostics", json.dumps({"enable": True})),
        (
            "post",
            "/nodes/registered_methods",
            json.dumps({"node_id": "1", "method_name": "a", "params": {}}),
        ),
        ("post", "/nodes/stop", json.dumps({})),
        # ("post", "/packages/load", json.dumps({"packages": []})),
        # ("post", "/shutdown", json.dumps({})),
    ],
)
async def test_http_server_routes(http_server, route_type, route, payload):

    async with aiohttp.ClientSession() as client:
        if route_type == "post":
            async with client.post(
                url=f"{http_server.url}{route}", data=payload
            ) as resp:
                assert resp.ok
        elif route_type == "get":
            async with client.get(url=f"{http_server.url}{route}") as resp:
                assert resp.ok


@pytest.mark.parametrize(
    "signal, payload",
    [
        (NODE_MESSAGE.STATUS, NodeState(logdir=None).to_dict()),
        (
            NODE_MESSAGE.REPORT_GATHER,
            {
                "node_id": "test",
                "latest_value": DataChunk().to_json(),
            },
        ),
        (
            NODE_MESSAGE.REPORT_RESULTS,
            {"success": True, "output": 1, "node_id": "test"},
        ),
        (
            NODE_MESSAGE.DIAGNOSTICS,
            {"node_id": "test", "diagnostics": NodeDiagnostics().to_dict()},
        ),
    ],
)
async def test_ws_signals(http_server, ws_client, signal, payload):
    await ws_client.async_send(signal=signal, data=payload, ok=True)
