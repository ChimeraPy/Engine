import logging
from logging.handlers import QueueHandler, QueueListener
from typing import Tuple

from .common import HandlerFactory
from .portable_queue import PortableQueue as Queue

LISTENER_QUEUES = dict()


class LogsQueueHandler:
    """A queue handler that can be used to send logs to a process safe queue."""

    def __init__(
        self, handlers: Tuple[str, ...] = ("console",), level: int = logging.DEBUG
    ):
        queue = Queue(-1)
        super().__init__(queue)
        handlers = [HandlerFactory.get(handler, level) for handler in handlers]
        self.listener = QueueListener(queue, *handlers, respect_handler_level=True)

    def start(self) -> None:
        self.listener.start()

    def stop(self) -> None:
        self.listener.stop()

    def is_listening(self) -> bool:
        return hasattr(self.listener, "_thread") and self.listener._thread.is_alive()

    def add_queue_handler(self, logger) -> None:
        existing_handlers = filter(
            lambda h: isinstance(h, QueueHandler), logger.handlers
        )
        map(logger.removeHandler, existing_handlers)
        if not self.is_listening():
            self.start()

        hdlr = QueueHandler(self.listener.queue)
        hdlr.setLevel(logger.level)
        logger.addHandler(hdlr)


def start_logs_queue_listener(queue_id: str) -> None:
    """Register a new queue handler for the given queue_id."""
    LISTENER_QUEUES[queue_id] = LogsQueueHandler()
    LISTENER_QUEUES[queue_id].start()


def add_queue_handler(queue_id, logger: logging.Logger):
    """Given a queue_id and a logger, add a queue handler to the logger."""
    if queue_id not in LISTENER_QUEUES:
        raise ValueError(
            f"Queue {queue_id} not found. Please call start_queue_handler first."
        )

    LISTENER_QUEUES[queue_id].add_queue_handler(logger)


def stop_logs_queue_listener(queue_id: str) -> None:
    """Stop the queue handler for the given queue_id."""
    if queue_id not in LISTENER_QUEUES:
        raise ValueError(
            f"Queue {queue_id} not found. Please call start_queue_handler first."
        )

    LISTENER_QUEUES[queue_id].stop()
    del LISTENER_QUEUES[queue_id]
