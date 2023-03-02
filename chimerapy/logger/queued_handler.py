import logging
import multiprocessing
from logging.handlers import QueueHandler, QueueListener
from typing import Tuple

from .common import HandlerFactory
from .portable_queue import PortableQueue as Queue
import queue
from typing import Optional
from logging import LogRecord


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
        logger.handlers.clear()
        logger.propagate = (
            False  # Prevent the log messages from being duplicated in parent
        )
        hdlr = QueueHandler(q)
        hdlr.setLevel(logger.level)
        logger.addHandler(hdlr)


def remove_queue_handler(logger: logging.Logger) -> None:
    """Given a logger, remove all queue handlers from the logger if they exist."""
    existing_handlers = filter(lambda h: isinstance(h, QueueHandler), logger.handlers)
    map(logger.removeHandler, existing_handlers)
