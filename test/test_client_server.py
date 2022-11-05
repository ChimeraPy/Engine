from typing import Dict
import time
import socket
import logging

import pytest
import numpy as np

import pdb

import chimerapy as cp

logger = logging.getLogger("chimerapy")

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


@pytest.mark.repeat(10)
def test_server_compression_decompression_not_missing_data(server, client):

    # Information that is causing an issue
    # nodes_server_table = {'screen': {'host': '127.0.1.1', 'port': 5000}, 'combine': {'host': '127.0.1.1', 'port': 5010}, 'web': {'host': '127.0.1.1', 'port': 5020}}
    # msg = {'signal': 'MANAGER_BROADCAST_NODE_SERVER_DATA', 'data': {'screen': {'host': '127.0.1.1', 'port': 5000}, 'combine': {'host': '127.0.1.1', 'port': 5010}, 'web': {'host': '127.0.1.1', 'port': 5020}}}

    # msg_bytes = b'\x1f\x8b\x08\x00j(Gc\x02\xff\x8d\x8c\xbb\x0e\x82@\x10E\x7f\x85l-d\x16%>\xbaU\x88\x15\x98\x80\xb1%\xb3\x0f\x95(\x8f\xc0\x12c\x0c\xff\xee\x8e\x95%\xc9\x14\xf7\x9e93\x1ff\xdf\x9da;\x8f\xa5"\x13\xc7$/\xd3\xa4(\\`\x0b\x8f\r\xd5\xad\xc1\xe7\xffr\x9f\x9fD|\x10\xc5\xb9\xccNqR\x16I~q4\x16gA\xbe\xadj3X\xac;:\x81\x1d\xd0\x10\x1f\xc7J\x13\xda*\xe0:\x02\xe5\xab\xe5U\xfb+\x89k_\x9a\x8d\xf2Q\xca\x10\xb4\x94K\x8c\x90|\x8d\x16\x9d\xffa\x83\xea\x8di~\xf1\xde\x0e\x96~\xf0p\x1d@\xc0\x03Nb\xd7\xf6\x04#\x00\x98\\Um-\xab\xc6\xcc\xf0\xf9\xcf\x7f\x199\xc3\ra"\x19\xd5\xc3U\x98\xbe\xd6\xe8\xbbr2\x01\x00\x00'

    # server.broadcast(
    #     {
    #         "signal": cp.enums.MANAGER_BROADCAST_NODE_SERVER_DATA,
    #         "data": nodes_server_table,
    #     }
    # )

    server.broadcast(
        {
            "signal": cp.enums.MANAGER_REQUEST_NODE_SERVER_DATA,
            "data": {},
        }
    )
