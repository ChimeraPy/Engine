import json

import aiohttp
import pytest

from chimerapy.engine.manager.http_server_service import HttpServerService
from chimerapy.engine.eventbus import EventBus, make_evented
from chimerapy.engine.states import ManagerState, WorkerState

from ..conftest import TEST_DATA_DIR


@pytest.fixture
async def http_server():

    # Creating the configuration for the eventbus and dataclasses
    event_bus = EventBus()
    state = make_evented(ManagerState(), event_bus=event_bus)

    # Create the services
    http_server = HttpServerService(
        name="http_server",
        port=0,
        enable_api=True,
        eventbus=event_bus,
        state=state,
    )
    await http_server.async_init()
    await http_server.start()
    return http_server


async def test_http_server_instanciate(http_server):
    ...


@pytest.mark.parametrize(
    "route, payload",
    [
        (
            "/workers/register",
            WorkerState(id="NULL", name="NULL", tempfolder=TEST_DATA_DIR).to_json(),
        ),
        (
            "/workers/deregister",
            WorkerState(id="NULL", name="NULL", tempfolder=TEST_DATA_DIR).to_json(),
        ),
        (
            "/workers/node_status",
            WorkerState(id="NULL", name="NULL", tempfolder=TEST_DATA_DIR).to_json(),
        ),
        ("/workers/send_archive", json.dumps({"worker_id": "test", "success": True})),
    ],
)
async def test_http_server_routes(http_server, route, payload):
    async with aiohttp.ClientSession() as client:
        async with client.post(url=f"{http_server.url}{route}", data=payload) as resp:
            assert resp.ok
