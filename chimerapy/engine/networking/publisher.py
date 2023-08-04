# Built-in Imports
import threading

# Third-party Imports
import zmq

# Internal Imports
from ..utils import get_ip_address
from .data_chunk import DataChunk

# Logging
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine-networking")

# Resources:
# https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html


class Publisher:
    def __init__(self):

        # Storing state variables
        self.port: int = 0
        self.host: str = get_ip_address()
        self.running: bool = False
        self._data_chunk: DataChunk = DataChunk()

    def __str__(self):
        return f"<Publisher@{self.host}:{self.port}>"

    def publish(self, data_chunk: DataChunk):
        self._data_chunk = data_chunk
        self._ready.set()

    def send_loop(self):

        while self.running:

            # # Wait until we have data to send!
            flag = self._ready.wait(timeout=1)
            if not flag:
                continue

            # Send
            self._zmq_socket.send(self._data_chunk._serialize())

            # Mark finish
            self._ready.clear()

    def start(self):

        # Mark that the producer is running
        self.running = True

        # Create the socket
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.PUB)
        self.port = self._zmq_socket.bind_to_random_port(f"tcp://{self.host}")
        # time.sleep(config.get("comms.timeout.pub-delay"))

        # Create send thread
        self._ready = threading.Event()
        self._ready.clear()
        self._send_thread = threading.Thread(target=self.send_loop)
        self._send_thread.start()

    def shutdown(self):

        # Mark to stop
        self.running = False
        self._send_thread.join()

        # Closing the socket
        self._zmq_socket.close()
        self._zmq_context.term()
