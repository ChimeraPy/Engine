import requests
import json

import pytest

from chimerapy.engine.manager.http_server_service import HttpServerService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, make_evented
from chimerapy.engine.states import ManagerState, WorkerState

from ..conftest import TEST_DATA_DIR


@pytest.fixture(scope="module")
def http_server():

    # Creating the configuration for the eventbus and dataclasses
    thread = AsyncLoopThread()
    thread.start()
    event_bus = EventBus(thread=thread)

    state = make_evented(ManagerState(), event_bus=event_bus)

    # Create the services
    http_server = HttpServerService(
        name="http_server",
        port=0,
        enable_api=True,
        thread=thread,
        eventbus=event_bus,
        state=state,
    )
    thread.exec(http_server.start()).result(timeout=10)
    return http_server


def test_http_server_instanciate(http_server):
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
def test_http_server_routes(http_server, route, payload):
    r = requests.post(f"{http_server.url}{route}", data=payload)
    assert r.status_code == requests.codes.ok
    assert http_server.eventbus._event_counts != 0
