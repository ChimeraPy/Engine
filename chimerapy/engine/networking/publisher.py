# Built-in Imports
import asyncio
from typing import Optional

# Third-party Imports
import zmq
import zmq.asyncio

# Logging
from chimerapy.engine import _logger

# Internal Imports
from ..utils import get_ip_address

logger = _logger.getLogger("chimerapy-engine-networking")

# Resources:
# https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html


class Publisher:
    def __init__(self, ctx: Optional[zmq.asyncio.Context] = None):

        # Parameters
        if ctx:
            self._zmq_context = ctx
        else:
            self._zmq_context = zmq.asyncio.Context()

        # Storing state variables
        self.port: int = 0
        self.host: str = get_ip_address()
        self._data: Optional[bytes] = None
        self._running: bool = False

    @property
    def running(self):
        return self._running

    def __str__(self):
        return f"<Publisher@{self.host}:{self.port}>"

    async def publish(self, data: bytes):
        self._data = data
        if not self._sending.is_set():
            self._sending.set()
            await self._zmq_socket.send(self._data)
            self._sending.clear()

    def start(self):

        # Create the socket
        self._zmq_socket = self._zmq_context.socket(zmq.PUB)
        self.port = self._zmq_socket.bind_to_random_port(f"tcp://{self.host}")
        self._running = True

        self._sending = asyncio.Event()
        self._sending.clear()

    def shutdown(self):

        # Closing the socket
        if self._running:
            self._zmq_socket.close()
            self._zmq_context.term()
            self._running = False
