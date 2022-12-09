from typing import Dict
import time
import socket
import logging
import pathlib
import os
import platform
import tempfile

import pytest
from pytest_lazyfixture import lazy_fixture
import numpy as np

import pdb

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
# cp.debug()

# Constants
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent
IMG_SIZE = 400


def echo(msg: Dict, s: socket.socket = None):
    ...


@cp.log
def show_image(msg: Dict, s: socket.socket = None):
    img = msg["data"]
    assert img.shape[0] == IMG_SIZE
    # cv2.imshow('test', img)
    # cv2.waitKey(0)


@pytest.fixture
def server():
    _server = cp.Server(
        port=9000,
        name="test",
        max_num_of_clients=10,
        sender_msg_type=cp.MANAGER_MESSAGE,
        accepted_msg_type=cp.WORKER_MESSAGE,
        handlers={"echo": echo, "image": show_image},
    )
    _server.start()
    yield _server
    _server.shutdown()


@pytest.fixture
def client(server):
    _client = cp.Client(
        host=server.host,
        port=server.port,
        name="test",
        connect_timeout=2,
        sender_msg_type=cp.WORKER_MESSAGE,
        accepted_msg_type=cp.MANAGER_MESSAGE,
        handlers={
            "echo": echo,
            "image": show_image,
            "SHUTDOWN": echo,
            "MANAGER_BROADCAST_NODE_SERVER_DATA": echo,
            "MANAGER_REQUEST_NODE_SERVER_DATA": echo,
        },
    )
    _client.start()
    yield _client
    _client.shutdown()


def test_client_connect_to_server(client, server):
    ...


def test_client_send_to_server(server, client):

    # Send message from client
    client.send({"signal": "echo", "data": "ECHO!"})


def test_multiple_clients_send_to_server(server):

    clients = []
    for i in range(5):
        _client = cp.Client(
            host=server.host,
            port=server.port,
            name=f"test-{i}",
            connect_timeout=2,
            sender_msg_type=cp.WORKER_MESSAGE,
            accepted_msg_type=cp.MANAGER_MESSAGE,
            handlers={"echo": echo, "SHUTDOWN": echo},
        )
        _client.start()
        clients.append(_client)

    for _client in clients:
        _client.send({"signal": "echo", "data": "ECHO!"}, ack=True)

    for _client in clients:
        _client.shutdown()


def test_server_send_to_client(server, client):

    # Wait until client is connected
    while len(server.client_comms) <= 0:
        time.sleep(0.1)

    client_socket = list(server.client_comms.keys())[0]

    server.send(client_socket, {"signal": "echo", "data": "ECHO!"})


def test_send_large_items(server, client):

    # Wait until client is connected
    while len(server.client_comms) <= 0:
        time.sleep(0.1)

    client_socket = list(server.client_comms.keys())[0]

    # img = np.ones([IMG_SIZE, IMG_SIZE])
    img = np.random.rand(IMG_SIZE, IMG_SIZE, 3)

    server.send(client_socket, {"signal": "image", "data": img}, ack=True)


def test_server_broadcast_to_multiple_clients(server):

    clients = []
    for i in range(5):
        _client = cp.Client(
            host=server.host,
            port=server.port,
            name=f"test-{i}",
            connect_timeout=2,
            sender_msg_type=cp.WORKER_MESSAGE,
            accepted_msg_type=cp.MANAGER_MESSAGE,
            handlers={"echo": echo, "SHUTDOWN": echo},
        )
        _client.start()
        clients.append(_client)

    # Wait until all clients are connected
    while len(server.client_comms) <= 4:
        time.sleep(0.1)

    server.broadcast({"signal": "echo", "data": "ECHO!"}, ack=True)

    for _client in clients:
        _client.shutdown()


# @pytest.mark.repeat(10)
@pytest.mark.parametrize(
    "dir",
    [
        (TEST_DIR / "mock" / "data" / "simple_folder"),
        (TEST_DIR / "mock" / "data" / "chimerapy_logs"),
    ],
)
def test_client_sending_folder_to_server(server, client, dir):

    # Action
    client.send_folder(name="test", folderpath=dir)

    # Get the expected behavior
    miss_counter = 0
    while len(server.file_transfer_records.keys()) == 0:

        miss_counter += 1
        time.sleep(0.1)

        if miss_counter > 100:
            assert False, "File transfer failed after 10 second"


@pytest.mark.parametrize(
    "dir",
    [
        (TEST_DIR / "mock" / "data" / "simple_folder"),
        (TEST_DIR / "mock" / "data" / "chimerapy_logs"),
    ],
)
def test_server_boradcast_file_to_clients(server, dir):

    clients = []
    for i in range(5):
        _client = cp.Client(
            host=server.host,
            port=server.port,
            name=f"test-{i}",
            connect_timeout=2,
            sender_msg_type=cp.WORKER_MESSAGE,
            accepted_msg_type=cp.MANAGER_MESSAGE,
            handlers={"echo": echo, "SHUTDOWN": echo},
        )
        _client.start()
        clients.append(_client)

    # Wait until all clients are connected
    while len(server.client_comms) <= 4:
        time.sleep(0.1)

    # Broadcast file
    server.send_folder(name="test", folderpath=dir)

    # Check all clients files
    for client in clients:
        miss_counter = 0
        while len(client.file_transfer_records.keys()) == 0:

            miss_counter += 1
            time.sleep(0.1)

            if miss_counter > 100:
                assert False, "File transfer failed after 10 second"

    for _client in clients:
        _client.shutdown()
