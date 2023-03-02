import logging
import multiprocessing
import os
from logging.handlers import QueueHandler, QueueListener
from typing import Tuple

from .common import HandlerFactory
from .portable_queue import PortableQueue as Queue
import queue
from typing import Optional
from logging import LogRecord


class InterceptedObjectId(logging.Filter):
    """A filter that adds the object id to the log record."""

    def filter(self, record: LogRecord) -> bool:
        record.object_id = os.getpid()
        return True


class MultiplexedQueueHandler(logging.Handler):
    """A queue handler that can be used to send logs to a process safe queue."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue_handlers = {}

    def register_queue_handler(self, q, object_id) -> None:
        if object_id not in self.queue_handlers:
            self.queue_handlers[object_id] = QueueHandler(q)

    def deregister_queue_handler(self, object_id) -> None:
        self.queue_handlers.pop(object_id, None)

    def emit(self, record: LogRecord) -> None:
        object_id = getattr(record, "object_id", None)
        if object_id and self.queue_handlers.get(object_id):
            self.queue_handlers[object_id].emit(record)


class PortableQueueListener(QueueListener):
    """A queue listener that can be used to send logs to a process safe queue."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._should_stop = False

    def dequeue(self, block: bool) -> Optional[LogRecord]:
        while True:
            try:
                logobj = self.queue.get(block=False)
                return logobj
            except queue.Empty:
                if self._should_stop:
                    return None

    def is_listening(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def enqueue_sentinel(self) -> None:
        self._should_stop = True

    def stop(self) -> None:
        if self.is_listening():
            super().stop()


def start_logs_queue_listener(
    handlers: Tuple[str, ...] = ("console",),
    level: int = logging.DEBUG,
) -> PortableQueueListener:
    """Start a queue listener in a new thread and return it."""
    q = Queue(-1)
    handlers = tuple(HandlerFactory.get(handler, level) for handler in handlers)
    listener = PortableQueueListener(q, *handlers, respect_handler_level=True)
    listener.start()
    return listener


def add_queue_handler(
    lock: multiprocessing.Lock, q: Queue, logger: logging.Logger
) -> None:
    """Add a queue handler to the given logger.

    This function will remove any existing handlers from the logger as well.
    """
    with lock:
        multiplexed_handler = None
        for handler in logger.handlers:
            if isinstance(handler, MultiplexedQueueHandler):
                multiplexed_handler = handler
                break
        if not multiplexed_handler:
            multiplexed_handler = MultiplexedQueueHandler()
            logger.addHandler(multiplexed_handler)

        multiplexed_handler.register_queue_handler(q, os.getpid())

        logger.propagate = (
            False  # Prevent the log messages from being duplicated in parent
        )
        logger.addFilter(InterceptedObjectId())  # Add the object id to the log record


def remove_queue_handler(logger: logging.Logger) -> None:
    """Given a logger, remove all queue handlers from the logger if they exist."""
    for handler in logger.handlers:
        if isinstance(handler, MultiplexedQueueHandler):
            handler.deregister_queue_handler(os.getpid())
