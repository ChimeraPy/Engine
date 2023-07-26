import requests

import pytest

from chimerapy.engine.manager.http_server_service import HttpServerService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, configure
from chimerapy.engine.states import ManagerState, WorkerState


@pytest.fixture
def http_server():

    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus()
    configure(eventbus, thread)

    state = ManagerState()

    # Create the services
    http_server = HttpServerService(
        name="http_server",
        port=0,
        enable_api=True,
        thread=thread,
        eventbus=eventbus,
        state=state,
    )
    http_server.start()
    return http_server


def test_http_server_instanciate():
    ...


@pytest.mark.parametrize(
    "route, payload",
    [
        ("/workers/register", WorkerState(id="NULL", name="NULL").to_json()),
        ("/workers/deregister", WorkerState(id="NULL", name="NULL").to_json()),
        ("/workers/node_status", WorkerState(id="NULL", name="NULL").to_json()),
    ],
)
def test_http_server_routes(http_server, route, payload):
    r = requests.post(f"{http_server.url}{route}", data=payload)
    assert r.status_code == requests.codes.ok
    assert http_server.eventbus._event_counts != 0
