import logging
import queue
from logging.handlers import QueueHandler
from multiprocessing import Process
from typing import Tuple

from .common import HandlerFactory
from .portable_queue import PortableQueue as Queue

LISTENER_QUEUES = dict()


class QueueListener(object):
    """
    This class implements an internal process listener which watches for
    LogRecords being added to a queue, removes them and passes them to a
    list of handlers for processing. This is a modified implementation of
    the QueueListener class from the logging.handlers module that uses a
    process instead of a thread.
    """

    _sentinel = None

    def __init__(self, q, *handlers, respect_handler_level=False):
        """
        Initialise an instance with the specified queue and
        handlers.
        """
        self.queue = q
        self.handlers = handlers
        self._process = None
        self.respect_handler_level = respect_handler_level

    def dequeue(self, block):
        """
        Dequeue a record and return it, optionally blocking.

        The base implementation uses get. You may want to override this method
        if you want to use timeouts or work with custom queue implementations.
        """
        return self.queue.get(block)

    def start(self):
        """
        Start the listener.

        This starts up a background process to monitor the queue for
        LogRecords to process.
        """
        self._process = t = Process(target=self._monitor)
        t.daemon = True
        t.start()

    def prepare(self, record):
        """
        Prepare a record for handling.

        This method just returns the passed-in record. You may want to
        override this method if you need to do any custom marshalling or
        manipulation of the record before passing it to the handlers.
        """
        return record

    def handle(self, record):
        """
        Handle a record.

        This just loops through the handlers offering them the record
        to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            if not self.respect_handler_level:
                process = True
            else:
                process = record.levelno >= handler.level
            if process:
                handler.handle(record)

    def _monitor(self):
        """
        Monitor the queue for records, and ask the handler
        to deal with them.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        q = self.queue
        has_task_done = hasattr(q, "task_done")
        while True:
            try:
                record = self.dequeue(True)
                if record is self._sentinel:
                    if has_task_done:
                        q.task_done()
                    break
                self.handle(record)
                if has_task_done:
                    q.task_done()
            except queue.Empty:
                break

    def enqueue_sentinel(self):
        """
        This is used to enqueue the sentinel record.

        The base implementation uses put_nowait. You may want to override this
        method if you want to use timeouts or work with custom queue
        implementations.
        """
        self.queue.put_nowait(self._sentinel)

    def stop(self):
        """
        Stop the listener.

        This asks the process to terminate, and then waits for it to do so.
        Note that if you don't call this before your application exits, there
        may be some records still left on the queue, which won't be processed.
        """
        self.enqueue_sentinel()
        self._process.join()
        self._process = None

    def is_listening(self):
        """Return True if the listener is running."""
        return self._process is not None and self._process.is_alive()


class LogsQueueHandler:
    """A queue handler that can be used to send logs to a process safe queue."""

    def __init__(
        self,
        queue_id: str,
        handlers: Tuple[str, ...] = ("console",),
        level: int = logging.DEBUG,
    ):
        q = Queue()
        super().__init__()
        self.id = queue_id
        handlers = [HandlerFactory.get(handler, level) for handler in handlers]
        self.listener = QueueListener(q, *handlers, respect_handler_level=True)

    def start(self) -> None:
        self.listener.start()

    def stop(self) -> None:
        self.listener.stop()

    def is_listening(self) -> bool:
        self.listener.is_listening()

    def add_queue_handler(self, logger) -> None:
        """Add a queue handler to the given logger."""
        remove_queue_handler(logger)

        hdlr = QueueHandler(self.listener.queue)
        hdlr.setLevel(logger.level)
        logger.addHandler(hdlr)


def start_logs_queue_listener(queue_id: str) -> LogsQueueHandler:
    """Register a new queue handler for the given queue_id."""
    LISTENER_QUEUES[queue_id] = LogsQueueHandler(queue_id)
    LISTENER_QUEUES[queue_id].start()

    return LISTENER_QUEUES[queue_id]


def add_queue_handler(queue_id, logger: logging.Logger):
    """Given a queue_id and a logger, add a queue handler to the logger."""
    if queue_id not in LISTENER_QUEUES:
        raise ValueError(
            f"Queue {queue_id} not found. Please call start_queue_handler first."
        )

    LISTENER_QUEUES[queue_id].add_queue_handler(logger)


def remove_queue_handler(logger: logging.Logger):
    """Given a queue_id and a logger, remove a queue handler from the logger."""

    existing_handlers = filter(lambda h: isinstance(h, QueueHandler), logger.handlers)
    map(logger.removeHandler, existing_handlers)


def stop_logs_queue_listener(queue_id: str) -> None:
    """Stop the queue handler for the given queue_id."""
    if queue_id not in LISTENER_QUEUES:
        raise ValueError(
            f"Queue {queue_id} not found. Please call start_queue_handler first."
        )

    LISTENER_QUEUES[queue_id].stop()
    del LISTENER_QUEUES[queue_id]
