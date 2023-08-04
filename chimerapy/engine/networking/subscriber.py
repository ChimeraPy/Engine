# Built-in
from typing import Optional, Union
import threading

# Third-party Imports
import zmq

# Internal Imports
from .data_chunk import DataChunk

# Logging
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine-networking")

# Reference:
# https://pyzmq.readthedocs.io/en/latest/api/zmq.html?highlight=socket#polling


class Subscriber:
    def __init__(self, port: int, host: str):

        self.port: int = port
        self.host: str = host
        self.running: bool = False
        self._data_chunk: DataChunk = DataChunk()

        # Create socket
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.SUB)
        self._zmq_socket.setsockopt(zmq.CONFLATE, 1)
        self._zmq_socket.connect(f"tcp://{self.host}:{self.port}")
        self._zmq_socket.subscribe(b"")

    def __str__(self):
        return f"<Subscriber@{self.host}:{self.port}>"

    def receive_loop(self):

        while self.running:

            # Poll (list of tuples of (socket, event_mask))
            events = self._zmq_poller.poll(timeout=1000)

            # If there is incoming data, then we know that the SUB
            # socket has content
            if events:

                # Recv
                serial_data_chunk = self._zmq_socket.recv()
                self._data_chunk = DataChunk.from_bytes(serial_data_chunk)
                self._ready.set()

    def receive(
        self,
        check_period: Union[int, float] = 0.1,
        timeout: Optional[Union[int, float]] = None,
    ):

        counter = 0
        while self.running:

            flag = self._ready.wait(timeout=check_period)
            if not flag:  # miss
                counter += 1

                if timeout and timeout < counter * check_period:
                    raise TimeoutError(f"{self}: receive timeout!")
                else:
                    continue

            else:  # Hit
                return self._data_chunk

    def start(self):

        # Mark that the Subscriber is running
        self.running = True

        # Create poller to make smart non-blocking IO
        self._zmq_poller = zmq.Poller()
        self._zmq_poller.register(self._zmq_socket, zmq.POLLIN)

        # Create thread
        self._ready = threading.Event()
        self._ready.clear()
        self._receive_thread = threading.Thread(target=self.receive_loop)
        self._receive_thread.start()

    def shutdown(self):

        if self.running:

            # Stop the thread
            self.running = False
            self._receive_thread.join()

        # And then close the socket
        self._zmq_socket.close()
        self._zmq_context.term()
