from typing import Dict
import time
import socket
import logging

import pytest
import jsonpickle
import numpy as np
import cv2

import pdb

logger = logging.getLogger("chimerapy")

import chimerapy as cp

# Constants
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
        host=socket.gethostbyname(socket.gethostname()),
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
        handlers={"echo": echo, "image": show_image, "SHUTDOWN": echo},
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
