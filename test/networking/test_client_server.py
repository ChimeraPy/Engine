from typing import Dict
import time
import socket
import logging
import pathlib
import os
import platform
import tempfile
import uuid
from aiohttp import web
import requests

from concurrent.futures import wait, Future

import pytest
from pytest_lazyfixture import lazy_fixture
import numpy as np

import pdb

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent
IMG_SIZE = 400
NUMBER_OF_CLIENTS = 5


async def hello(request):
    return web.Response(text="Hello, world")


ECHO_FLAG = -11111


async def echo(msg: Dict, ws: web.WebSocketResponse = None):
    logger.debug("ECHO: " + str(msg))


@pytest.fixture
def server():
    server = cp.Server(
        name="test",
        port=0,
        routes=[web.get("/", hello)],
        ws_handlers={ECHO_FLAG: echo},
    )
    server.serve()
    yield server
    server.shutdown()


@pytest.fixture
def client(server):
    client = cp.Client(
        name="test",
        host=server.host,
        port=server.port,
        ws_handlers={ECHO_FLAG: echo},
    )
    client.connect()
    yield client
    client.shutdown()


@pytest.fixture
def client_list(server):

    clients = []
    for i in range(NUMBER_OF_CLIENTS):
        client = cp.Client(
            host=server.host,
            port=server.port,
            name=f"test-{i}",
            ws_handlers={ECHO_FLAG: echo},
        )
        client.connect()
        clients.append(client)

    yield clients

    for client in clients:
        client.shutdown()


def test_server_http_req_res(server):
    r = requests.get(f"http://{server.host}:{server.port}")
    assert r.status_code == 200 and r.text == "Hello, world"


def test_server_websocket_connection(server, client):
    assert client.name in list(server.ws_clients.keys())


def test_server_send_to_client(server, client):
    # Simple send
    server.send(client_name=client.name, signal=ECHO_FLAG, data="HELLO")

    # Simple send with OK
    server.send(client_name=client.name, signal=ECHO_FLAG, data="HELLO", ok=True)

    assert cp.utils.waiting_for(
        lambda: client.msg_processed_counter >= 2,
        timeout=2,
    )


def test_client_send_to_server(server, client):
    # Simple send
    client.send(signal=ECHO_FLAG, data="HELLO")

    # Simple send with OK
    client.send(signal=ECHO_FLAG, data="HELLO", ok=True)

    assert cp.utils.waiting_for(
        lambda: server.msg_processed_counter >= 2,
        timeout=2,
    )


def test_multiple_clients_send_to_server(server, client_list):

    for client in client_list:
        client.send(signal=ECHO_FLAG, data="ECHO!", ok=True)

    assert cp.utils.waiting_for(
        lambda: server.msg_processed_counter >= NUMBER_OF_CLIENTS,
        timeout=5,
    )


def test_server_broadcast_to_multiple_clients(server, client_list):

    server.broadcast(signal=ECHO_FLAG, data="ECHO!", ok=True)

    for client in client_list:
        assert cp.utils.waiting_for(
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
def test_client_sending_folder_to_server(server, client, dir):

    # Action
    client.send_folder(sender_name="test_worker", dir=dir)

    # Get the expected behavior
    miss_counter = 0
    while len(server.file_transfer_records.keys()) == 0:

        miss_counter += 1
        time.sleep(0.1)

        if miss_counter > 100:
            assert False, "File transfer failed after 10 second"

    # Also check that the file exists
    for record in server.file_transfer_records["test_worker"].values():
        assert record["dst_filepath"].exists()
