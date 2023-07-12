import logging
from typing import Optional, Tuple

import zmq

LOGGING_MAX_PORT = 60000
LOGGING_MIN_PORT = 50000


def bind_pull_socket(
    port: Optional[int] = None, transport="tcp", **kwargs
) -> Tuple[zmq.Socket, int]:
    """Bind a pull socket to the given port."""
    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    if port is None:
        port = socket.bind_to_random_port(
            f"{transport}://*",
            min_port=kwargs.get("min_port", LOGGING_MIN_PORT),
            max_port=kwargs.get("max_port", LOGGING_MAX_PORT),
            max_tries=kwargs.get("max_tries", 20),
        )
    else:
        if (
            (port < LOGGING_MIN_PORT or port > LOGGING_MAX_PORT)
            and transport == "tcp"
            and port != 0
        ):
            raise ValueError(
                f"Port {port} is out of range. Please use a port between \
                {LOGGING_MIN_PORT} and {LOGGING_MAX_PORT} "
                f"When using `tcp` transport."
            )
        socket.bind(f"{transport}://*:{port}")

    return socket, port


def connect_push_socket(ip: str, port: int, transport="tcp") -> zmq.Socket:
    """Connect a pull socket to the given ip and port."""
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(f"{transport}://{ip}:{port}")
    return socket


def get_unique_child_name(logger, name: str) -> str:
    """Get a unique child logger name."""
    j = 1
    unq = name

    while logging.root.manager.loggerDict.get(logger.name + "." + unq):
        unq = f"{name}_{j}"
        j += 1

    return unq
