import asyncio
import os
import shutil

import pytest

from chimerapy.engine.worker.http_client_service import HttpClientService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.server import Server
from chimerapy.engine.eventbus import EventBus, make_evented, Event
from chimerapy.engine.states import WorkerState, NodeState
from chimerapy.engine import _logger

from ..conftest import TEST_DATA_DIR, TEST_SAMPLE_DATA_DIR

logger = _logger.getLogger("chimerapy-engine")


@pytest.fixture
def server():
    server = Server(
        id="test_server",
        port=0,
    )
    server.serve()
    yield server
    server.shutdown()


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

    eventbus.send(Event("shutdown")).result()


def test_http_client_instanciate(http_client):
    ...


async def test_connect_via_ip(http_client, manager):
    assert await http_client._async_connect_via_ip(host=manager.host, port=manager.port)


async def test_connect_via_zeroconf(http_client, manager):
    await manager.async_zeroconf()
    assert await http_client._async_connect_via_zeroconf()


async def test_node_status_update(http_client, manager):
    assert await http_client._async_connect_via_ip(host=manager.host, port=manager.port)
    assert await http_client._async_node_status_update()


async def test_worker_state_changed_updates(http_client, manager):
    assert await http_client._async_connect_via_ip(host=manager.host, port=manager.port)

    # Change the state
    http_client.state.nodes["test"] = NodeState(id="test", name="test")

    # Wait for the update
    logger.debug("Sleeping for 1")
    await asyncio.sleep(1)

    # Check
    assert "test" in manager.state.workers[http_client.state.id].nodes


async def test_send_archive_locally(http_client):

    # Adding simple file
    test_file = http_client.state.tempfolder / "test.txt"
    if test_file.exists():
        os.remove(test_file)
    else:
        with open(test_file, "w") as f:
            f.write("hello")

    dst = TEST_DATA_DIR / "test_folder"
    os.makedirs(dst, exist_ok=True)

    new_folder_name = dst / f"{http_client.state.name}-{http_client.state.id}"
    if new_folder_name.exists():
        shutil.rmtree(new_folder_name)

    dst_path = await http_client._send_archive_locally(dst)

    dst_test_file = dst_path / "test.txt"
    assert dst_path.exists() and dst_test_file.exists()

    with open(dst_test_file, "r") as f:
        assert f.read() == "hello"


async def test_send_archive_remotely(http_client, server):

    # Make a copy of example logs
    shutil.copytree(
        str(TEST_SAMPLE_DATA_DIR / "chimerapy_logs"),
        str(http_client.state.tempfolder / "data"),
    )

    assert await http_client._send_archive_remotely(server.host, server.port)

    for record in server.file_transfer_records.records.values():
        assert record.sender_id == http_client.state.name
