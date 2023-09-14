from typing import Dict
import tempfile
import pathlib
import asyncio
import os
import enum
import aiohttp
from aiohttp import web

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.networking import Server, Client

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()

# Constants
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent
IMG_SIZE = 400
NUMBER_OF_CLIENTS = 5


async def hello(request):
    return web.Response(text="Hello, world")


class TEST_PROTOCOL(enum.Enum):
    ECHO_FLAG = -11111


async def echo(msg: Dict, ws: web.WebSocketResponse = None):
    logger.debug("ECHO: " + str(msg))


@pytest.fixture
async def server():
    server = Server(
        id="test_server",
        port=0,
        routes=[web.get("/", hello)],
        ws_handlers={TEST_PROTOCOL.ECHO_FLAG: echo},
    )
    await server.async_serve()
    yield server
    await server.async_shutdown()


@pytest.fixture
async def client(server):
    client = Client(
        id="test_client",
        host=server.host,
        port=server.port,
        ws_handlers={TEST_PROTOCOL.ECHO_FLAG: echo},
    )
    await client.async_connect()
    yield client
    await client.async_shutdown()


@pytest.fixture
async def client_list(server):

    clients = []
    for i in range(NUMBER_OF_CLIENTS):
        client = Client(
            host=server.host,
            port=server.port,
            id=f"test-{i}",
            ws_handlers={TEST_PROTOCOL.ECHO_FLAG: echo},
        )
        await client.async_connect()
        clients.append(client)

    yield clients

    for client in clients:
        await client.async_shutdown()


async def test_server_instanciate(server):
    ...


async def test_server_http_req_res(server):
    url = f"http://{server.host}:{server.port}"
    async with aiohttp.ClientSession(url) as session:
        async with session.get("/") as resp:
            assert resp.ok


async def test_server_websocket_connection(server, client):
    assert client.id in list(server.ws_clients.keys())


async def test_async_ws(server):
    client = Client(
        id="test_client",
        host=server.host,
        port=server.port,
        ws_handlers={TEST_PROTOCOL.ECHO_FLAG: echo},
    )
    await client.async_connect()
    assert client.id in list(server.ws_clients.keys())
    await client.async_shutdown()


async def test_server_websocket_connection_shutdown(server, client):
    await client.async_shutdown()
    await asyncio.sleep(0.1)
    await server.async_broadcast(signal=TEST_PROTOCOL.ECHO_FLAG, data="ECHO!")


async def test_server_send_to_client(server, client):
    # Simple send
    await server.async_send(
        client_id=client.id, signal=TEST_PROTOCOL.ECHO_FLAG, data="HELLO"
    )

    # Simple send with OK
    await server.async_send(
        client_id=client.id, signal=TEST_PROTOCOL.ECHO_FLAG, data="HELLO", ok=True
    )

    assert await cpe.utils.async_waiting_for(
        lambda: client.msg_processed_counter >= 2,
        timeout=2,
    )


async def test_client_send_to_server(server, client):
    # Simple send
    await client.async_send(signal=TEST_PROTOCOL.ECHO_FLAG, data="HELLO")

    # Simple send with OK
    await client.async_send(signal=TEST_PROTOCOL.ECHO_FLAG, data="HELLO", ok=True)

    assert await cpe.utils.async_waiting_for(
        lambda: server.msg_processed_counter >= 2,
        timeout=2,
    )


async def test_multiple_clients_send_to_server(server, client_list):

    for client in client_list:
        await client.async_send(signal=TEST_PROTOCOL.ECHO_FLAG, data="ECHO!", ok=True)

    assert await cpe.utils.async_waiting_for(
        lambda: server.msg_processed_counter >= NUMBER_OF_CLIENTS,
        timeout=5,
    )


async def test_server_broadcast_to_multiple_clients(server, client_list):

    await server.async_broadcast(signal=TEST_PROTOCOL.ECHO_FLAG, data="ECHO!", ok=True)

    for client in client_list:
        assert await cpe.utils.async_waiting_for(
            lambda: client.msg_processed_counter >= 2,
            timeout=5,
        )


@pytest.mark.parametrize(
    "dir",
    [
        (TEST_DIR / "mock" / "data" / "simple_folder"),
        (TEST_DIR / "mock" / "data" / "chimerapy_logs"),
    ],
)
async def test_client_sending_folder_to_server(server, client, dir):

    # Action
    await client.async_send_folder(sender_id="test_worker", dir=dir)

    # Also check that the file exists
    for record in server.file_transfer_records.records.values():
        assert record.location.exists()

    # Test moving the files to a logdir
    temp = pathlib.Path(tempfile.mkdtemp())
    await server.move_transferred_files(temp)
    await server.move_transferred_files(temp, owner="test_worker", owner_id="asdf")
