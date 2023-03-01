import logging
from logging.handlers import QueueHandler, QueueListener
from typing import Tuple

from .common import HandlerFactory
from .portable_queue import PortableQueue as Queue


class LogsQueueListener:
    """A queue listener that can be used to send logs to a process safe queue."""

    def __init__(
        self,
        queue: Queue,
        handlers: Tuple[str, ...] = ("console",),
        level: int = logging.DEBUG,
    ):
        handlers = [HandlerFactory.get(handler, level) for handler in handlers]
        self.listener = QueueListener(queue, *handlers, respect_handler_level=True)

    def start(self) -> None:
        self.listener.start()

    def stop(self) -> None:
        self.listener.stop()

    def is_listening(self) -> bool:
        return self.listener._thread is not None and self.listener._thread.is_alive()

    @property
    def queue(self) -> Queue:
        return self.listener.queue


def start_logs_queue_listener(
    handlers: Tuple[str, ...] = ("console",), level: int = logging.DEBUG
) -> LogsQueueListener:
    """Start a queue listener in a new thread and return it."""
    queue = Queue(-1)
    listener = LogsQueueListener(queue, handlers, level)
    listener.start()
    return listener


def add_queue_handler(queue: Queue, logger: logging.Logger) -> None:
    """Add a queue handler to the given logger."""
    remove_queue_handler(logger)
    hdlr = QueueHandler(queue)
    hdlr.setLevel(logger.level)


def remove_queue_handler(logger: logging.Logger):
    """Given a logger, remove all queue handlers from the logger if they exist."""
    existing_handlers = filter(lambda h: isinstance(h, QueueHandler), logger.handlers)
    map(logger.removeHandler, existing_handlers)
