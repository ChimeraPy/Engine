import asyncio
import logging
import os
import shutil
from typing import Any

import pytest
from aiodistbus import make_evented

from chimerapy.engine import _logger
from chimerapy.engine.networking.server import Server
from chimerapy.engine.states import NodeState, WorkerState
from chimerapy.engine.worker.http_client_service import HttpClientService

from ..conftest import TEST_DATA_DIR, TEST_SAMPLE_DATA_DIR

logger = _logger.getLogger("chimerapy-engine")


@pytest.fixture
async def server():
    server = Server(
        id="test_server",
        port=0,
    )
    await server.async_serve()
    yield server
    await server.async_shutdown()


async def handler(*args, **kwargs):
    logger.debug("Received data")
    logger.debug(f"{args}, {kwargs}")


@pytest.fixture
async def http_client(bus, entrypoint):

    # Requirements
    state = WorkerState()
    state = make_evented(state, bus=bus)
    logger = _logger.getLogger("chimerapy-engine-worker")
    log_receiver = _logger.get_node_id_zmq_listener()
    log_receiver.start(register_exit_handlers=True)

    # Create the services
    http_client = HttpClientService(
        name="http_client",
        state=state,
        logger=logger,
        logreceiver=log_receiver,
    )
    await http_client.attach(bus)
    yield http_client
    await entrypoint.emit("shutdown")


async def test_http_client_instanciate(http_client):
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
