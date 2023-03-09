import os
from logging.handlers import QueueHandler, QueueListener
import zmq

from .utils import bind_pull_socket, connect_push_socket
from .common import HandlerFactory
from logging import LogRecord, makeLogRecord
from typing import Optional


class ZMQListener(QueueListener):
    def __init__(
        self,
        port: Optional[int] = None,
        handlers=None,
        respect_handler_level: bool = True,
    ):
        socket, port = bind_pull_socket(port)
        handlers = handlers or [
            HandlerFactory.get("console")
        ]  # For now, only console handler
        super().__init__(socket, *handlers, respect_handler_level=respect_handler_level)
        self._should_stop = False
        self.port = port

    def dequeue(self, block: bool) -> Optional[LogRecord]:
        while True:
            try:
                logobj = self.queue.recv_json(zmq.NOBLOCK)
                return makeLogRecord(logobj)
            except zmq.Again:
                if self._should_stop:
                    return None

    def enqueue_sentinel(self) -> None:
        self._should_stop = True

    def stop(self) -> None:
        super().stop()

    def is_running(self):
        return self._thread.is_alive()


class NodeIDZMQListener(ZMQListener):
    def __init__(self, port: Optional[int] = None, respect_handler_level: bool = True):
        handlers = (HandlerFactory.get("console-node_id"),)
        super().__init__(port, handlers, respect_handler_level=respect_handler_level)


class ZMQHandler(QueueHandler):
    def __init__(self, host: str, port: int):
        socket = connect_push_socket(host, port)
        super().__init__(socket)

    def emit(self, record: LogRecord) -> None:
        self.queue.send_json(record.__dict__)


class NodeIdZMQHandler(ZMQHandler):
    def __init__(
        self, host: str, port: int, node_id_callback: Optional[callable] = os.getpid
    ):
        super().__init__(host, port)
        self.node_id_callback = node_id_callback
        self.node_ids = {}

    def emit(self, record: LogRecord) -> None:
        record.node_id = self.node_ids.get(self.node_id_callback(), None)
        super().emit(record)

    def register_node_id(self, node_id: str):
        self.node_ids[self.node_id_callback()] = node_id
