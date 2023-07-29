import asyncio


import pytest

from chimerapy.engine.worker.http_client_service import HttpClientService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, make_evented, Event
from chimerapy.engine.states import WorkerState, NodeState
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine")


@pytest.fixture
def http_client():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Requirements
    state = make_evented(WorkerState(), event_bus=eventbus)
    logger = _logger.getLogger("chimerapy-engine-worker")
    log_receiver = _logger.get_node_id_zmq_listener()
    log_receiver.start(register_exit_handlers=True)

    # Create the services
    http_client = HttpClientService(
        name="http_client",
        state=state,
        eventbus=eventbus,
        logger=logger,
        logreceiver=log_receiver,
    )
    yield http_client

    eventbus.send(Event("shutdown"))


def test_http_client_instanciate(http_client):
    ...


@pytest.mark.asyncio
async def test_connect_via_ip(http_client, manager):
    assert await http_client._async_connect_via_ip(host=manager.host, port=manager.port)


@pytest.mark.asyncio
async def test_connect_via_zeroconf(http_client, manager):
    await manager.async_zeroconf()
    assert await http_client._async_connect_via_zeroconf()


@pytest.mark.skip(reason="Failed to shutdown, manager-side error in API call.")
@pytest.mark.asyncio
async def test_node_status_update(http_client, manager):
    assert await http_client._async_connect_via_ip(host=manager.host, port=manager.port)
    assert await http_client._async_node_status_update()


@pytest.mark.skip(reason="manager-side error")
@pytest.mark.asyncio
async def test_worker_state_changed_updates(http_client, manager):
    assert await http_client._async_connect_via_ip(host=manager.host, port=manager.port)

    # Change the state
    http_client.state.nodes["test"] = NodeState(id="test", name="test")
    # http_client.state.ip = '17'

    # Wait for the update
    logger.debug("Sleeping for 5")
    await asyncio.sleep(5)

    # Check
    logger.debug(http_client.state)
    logger.debug(manager.state.workers[http_client.state.id])
    # assert manager.state.workers[http_client.state.id] == http_client.state
