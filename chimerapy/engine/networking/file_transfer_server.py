import os
from asyncio import Event
from pathlib import Path

import zmq
import zmq.asyncio

from chimerapy.engine import config as cpe_config


def set_socket_hwm(socket: zmq.Socket, hwm: int):
    socket.sndhwm = hwm
    socket.rcvhwm = hwm


class FileTransferServer:
    """A server for file transfer with ZeroMQ Router-Dealer pattern."""

    def __init__(self, port: int, context: zmq.asyncio.Context):
        self._port = port
        self._context = context
        self._fp = None
        self._stop_event = Event()

    async def mount(self, filepath: Path):
        assert filepath.exists(), f"File {filepath} does not exist."
        self._fp = filepath.open("rb")

    async def start(self):
        assert self._fp is not None, "No file mounted."
        socket = self._context.socket(zmq.ROUTER)
        set_socket_hwm(socket, cpe_config.get("comms.file-transfer.max-chunks"))
        socket.bind(f"tcp://*:{self._port}")
        print(f"File transfer server started on port {self._port}.")
        while True:
            try:
                msg = await socket.recv_multipart()
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break
                else:
                    raise e

            identity, command, offset_str, chunksize_str = msg

            assert command == b"fetch", f"Unknown command {command}."

            offset = int(offset_str)
            chunksize = int(chunksize_str)

            self._fp.seek(offset, os.SEEK_SET)
            data = self._fp.read(chunksize)

            if not data:
                await socket.send_multipart([identity, b"okay"])
                continue
            else:
                await socket.send_multipart([identity, data])


if __name__ == "__main__":
    import asyncio
    import sys

    port = int(sys.argv[1])
    filepath = Path(sys.argv[2])

    context = zmq.asyncio.Context()
    server = FileTransferServer(port, context)
    asyncio.run(server.mount(filepath))
    asyncio.run(server.start())
