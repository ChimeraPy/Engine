import json

import aiohttp
import pytest
from aiodistbus import make_evented

from chimerapy.engine.manager.http_server_service import HttpServerService
from chimerapy.engine.states import ManagerState, WorkerState

from ..conftest import TEST_DATA_DIR


@pytest.fixture
async def http_server(bus):

    # Creating the configuration for the eventbus and dataclasses
    state = make_evented(ManagerState(), bus=bus)

    # Create the services
    http_server = HttpServerService(
        name="http_server",
        port=0,
        enable_api=True,
        state=state,
    )
    await http_server.attach(bus)
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
