from logging.handlers import QueueHandler, QueueListener
import zmq

from .utils import bind_pull_socket, connect_push_socket
from .common import HandlerFactory
from logging import LogRecord, makeLogRecord
from typing import Optional


class ZMQListener(QueueListener):
    def __init__(self, port, respect_handler_level: bool = True):
        socket = bind_pull_socket(port)
        handlers = [HandlerFactory.get("console")]
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
        self.queue.close()

    def is_running(self):
        return self._thread.is_alive()


class ZMQHandler(QueueHandler):
    def __init__(self, host: str, port: int):
        socket = connect_push_socket(host, port)
        super().__init__(socket)

    def emit(self, record: LogRecord) -> None:
        self.queue.send_json(record.__dict__)
