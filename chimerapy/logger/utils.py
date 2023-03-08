import zmq


def bind_pull_socket(port: int, transport="tcp") -> zmq.Socket:
    """Bind a pull socket to the given port."""
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    print(f"{transport}://*:{port}")
    socket.bind(f"{transport}://*:{port}")
    return socket


def connect_push_socket(ip: str, port: int, transport="tcp") -> zmq.Socket:
    """Connect a pull socket to the given ip and port."""
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(f"{transport}://{ip}:{port}")
    return socket
