from typing import Dict
import socket

import chimerapy as cp


def null(msg: Dict = {}, s: socket.socket = None):
    ...


def multiple_incoming_video_streams():

    server = cp.Server(
        port=9000,
        name="test",
        max_num_of_clients=10,
        sender_msg_type=cp.MANAGER_MESSAGE,
        accepted_msg_type=cp.WORKER_MESSAGE,
        handlers={"echo": null},
    )

    clients = []
    for i in range(5):
        client = cp.Client(
            host=server.host,
            port=server.port,
            name=f"test-{i}",
            connect_timeout=2,
            sender_msg_type=cp.WORKER_MESSAGE,
            accepted_msg_type=cp.MANAGER_MESSAGE,
            handlers={"echo": null, "SHUTDOWN": null},
        )
        client.start()
        clients.append(client)

    for client in clients:
        client.send({"signal": "echo", "data": "ECHO!"}, ack=True)

    for client in clients:
        client.shutdown()

    server.shutdown()
