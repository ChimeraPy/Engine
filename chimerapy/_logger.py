# Setup the logging for the library
# References:
# https://docs.python-guide.org/writing/logging/
# https://stackoverflow.com/questions/13649664/how-to-use-logging-with-pythons-fileconfig-and-configure-the-logfile-filename
import atexit
import logging.config
import logging.handlers
import os
import queue
import threading
from dataclasses import dataclass
from logging import LogRecord
from multiprocessing import Queue
from typing import Any, Dict

from zmq.log.handlers import TOPIC_DELIM, PUBHandler


# FixMe: This is a hack. The ZMQ PUBHandler should be able to handle non-strings and
#  brings in an overhead for tracking it upstream
class ZMQLogPublisher(PUBHandler):
    """A small wrapper around the ZMQ PUBHandler to make it work with non-strings."""

    def emit(self, record: LogRecord) -> None:
        """Emit a log message on my socket."""

        try:
            topic, record.msg = record.getMessage().split(TOPIC_DELIM, 1)
        except ValueError:
            topic = ""
        try:
            bmsg = self.format(record).encode("utf8")
        except Exception:
            self.handleError(record)
            return

        topic_list = []

        if self.root_topic:
            topic_list.append(self.root_topic)

        topic_list.append(record.levelname)

        if topic:
            topic_list.append(topic)

        btopic = ".".join(topic_list).encode("utf8")

        self.socket.send_multipart([btopic, bmsg])


@dataclass
class ZMQLogHandlerConfig:
    """Configuration for the log publishing via ZMQ Sockets."""

    publisher_port: int = 8687
    transport: str = "ws"
    root_topic: str = "chimerapy_logs"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        kwargs = {
            "publisher_port": d.get("publisher_port", 8687),
            "transport": d.get("publisher_transport", "ws"),
            "root_topic": d.get("publisher_root_topic", "chimerapy_logs"),
        }
        return cls(**kwargs)


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "chimerapy": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True,
        },
        "chimerapy-worker": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True,
        },
        "chimerapy-networking": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True,
        },
    },
}


# Setup the logging configuration
def setup():
    # Setting up the configureation
    logging.config.dictConfig(LOGGING_CONFIG)
    atexit.register(stop_process_logger)


def add_zmq_handler(logger: logging.Logger, handler_config: ZMQLogHandlerConfig):
    """Add a ZMQ log handler to the logger.

    Note:
        Uses the same formatter as the consoleHandler
    """
    # Add a handler to publish the logs to zmq ws
    handler = ZMQLogPublisher(
        f"{handler_config.transport}://*:{handler_config.publisher_port}"
    )
    handler.root_topic = handler_config.root_topic
    logger.addHandler(handler)
    handler.setLevel(logging.DEBUG)
    # Use the same formatter as the console
    handler.setFormatter(
        logging.Formatter(
            logger.handlers[0].formatter._fmt,
            logger.handlers[0].formatter.datefmt,
        )
    )  # FIXME: This is a hack, can this be done better?


def getLogger(
    name: str,
) -> logging.Logger:
    # Get the logging
    logger = logging.getLogger(name)

    # Ensure that the configuration is set
    debug_loggers = os.environ.get("CHIMERAPY_DEBUG_LOGGERS", "").split(os.pathsep)
    if name in debug_loggers:
        logger.setLevel(logging.DEBUG)
    if logger.name == "chimerapy-worker":
        start_process_logger()
    return logger


class ThreadedQueueLogger(threading.Thread):
    """A threaded logger that logs messages from a queue."""

    _global_process_logger = None
    _END_OF_LOGGING = "END_OF_LOGGING"

    def __init__(self):
        super().__init__()
        self.daemon = True
        self.queue = Queue(-1)
        self.sentinel_enqueued = False

    @classmethod
    def get_global_process_logger(cls) -> "ThreadedQueueLogger":
        if cls._global_process_logger is not None:
            return cls._global_process_logger
        raise RuntimeError("Global process logger not set")

    @classmethod
    def create_global_process_logger(cls) -> "ThreadedQueueLogger":
        cls._global_process_logger = cls()
        return cls._global_process_logger

    @staticmethod
    def has_global_process_logger() -> bool:
        return ThreadedQueueLogger._global_process_logger is not None

    @staticmethod
    def configure() -> None:
        root = logging.getLogger("chimerapy-node-logger")
        h = logging.StreamHandler()
        f = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - ID - %(node_id)s: %(message)s"
        )  # ToDO: Can this be done better?
        h.setFormatter(f)
        h.setLevel(logging.DEBUG)
        root.setLevel(logging.DEBUG)
        root.addHandler(h)
        root.propagate = False

    def run(self) -> None:
        self.configure()
        while True:
            try:
                if self.sentinel_enqueued:
                    break
                record = self.queue.get(block=False)
                if record == self._END_OF_LOGGING:
                    break
                logger = logging.getLogger("chimerapy-node-logger")
                logger.handle(record)
            except queue.Empty:
                pass
            except Exception:
                import sys
                import traceback

                traceback.print_exc(file=sys.stderr)


class NodeIdFilter(logging.Filter):
    """A filter that adds the node_id to the log record."""

    def __init__(self, node_id: str):
        super().__init__()
        self.node_id = node_id

    def filter(self, record: logging.LogRecord):
        record.node_id = self.node_id
        return True


def configure_new_node(node_id):
    """Configure a new node logger."""
    process_logger = get_process_logger()
    log_process_queue = process_logger.queue

    h = logging.handlers.QueueHandler(log_process_queue)
    root = list(map(lambda name: logging.getLogger(name), ["chimerapy-node"])).pop()
    root.addHandler(h)
    root.propagate = False
    root.addFilter(NodeIdFilter(str(node_id)))
    root.setLevel(logging.DEBUG)
    return root


def start_process_logger() -> ThreadedQueueLogger:
    """Start the process logger if it is not already started."""
    if not ThreadedQueueLogger.has_global_process_logger():
        process_logger = ThreadedQueueLogger.create_global_process_logger()
        process_logger.start()

    return ThreadedQueueLogger.get_global_process_logger()


def get_process_logger() -> ThreadedQueueLogger:
    """Get the process logger."""
    return ThreadedQueueLogger.get_global_process_logger()


def stop_process_logger() -> None:
    """Stop the process logger if it is started and there are no more active loggers."""
    if ThreadedQueueLogger.has_global_process_logger():
        queue_logger = ThreadedQueueLogger.get_global_process_logger()

        queue_logger.queue.put(ThreadedQueueLogger._END_OF_LOGGING)
        queue_logger.sentinel_enqueued = True
        queue_logger.join()
        ThreadedQueueLogger._global_process_logger = None
