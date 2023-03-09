from typing import Tuple, Optional

import zmq


def bind_pull_socket(
    port: Optional[int] = None, transport="tcp", **kwargs
) -> Tuple[zmq.Socket, int]:
    """Bind a pull socket to the given port."""
    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    if port is None:
        port = socket.bind_to_random_port(
            f"{transport}://*",
            min_port=kwargs.get("min_port", 50000),
            max_port=kwargs.get("max_port", 60000),
            max_tries=kwargs.get("max_tries", 20),
        )
    else:
        socket.bind(f"{transport}://*:{port}")

    return socket, port


def connect_push_socket(ip: str, port: int, transport="tcp") -> zmq.Socket:
    """Connect a pull socket to the given ip and port."""
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(f"{transport}://{ip}:{port}")
    return socket
