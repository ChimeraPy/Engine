# Setup the logging for the library
# References:
# https://docs.python-guide.org/writing/logging/
# https://stackoverflow.com/questions/13649664/how-to-use-logging-with-pythons-fileconfig-and-configure-the-logfile-filename
import logging.config
import os
from dataclasses import dataclass
from logging import LogRecord
from typing import Any, Dict, Optional

from zmq.log.handlers import TOPIC_DELIM, PUBHandler
from logging import StreamHandler
from .logger.zmq_handlers import NodeIDZMQListener, NodeIdZMQHandler
from .logger.common import HandlerFactory


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
        "chimerapy-subprocess": {
            "handlers": [],
            "level": "DEBUG",
            "propagate": True,
        },
    },
}


# Setup the logging configuration
def setup():

    # Setting up the configureation
    logging.config.dictConfig(LOGGING_CONFIG)


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

    return logger


def get_node_id_zmq_listener(port: Optional[int] = None) -> NodeIDZMQListener:
    """Get a ZMQ pull listener on the given port."""
    listener = NodeIDZMQListener(port)
    return listener


def add_console_handler(logger: logging.Logger) -> None:
    """Add a console handler to the logger.

    Note:
        Uses the same formatter as the consoleHandler in logging
    """
    exists = any(isinstance(h, StreamHandler) for h in logger.handlers)
    if not exists:
        hdlr = HandlerFactory.get_console_handler()
        logger.addHandler(hdlr)


def add_node_id_zmq_push_handler(
    logger: logging.Logger, ip: str, port: int, node_id
) -> None:
    """Add a ZMQ log handler to the logger.

    Note:
        Uses the same formatter as the consoleHandler in logging
    """
    # Add a handler to publish the logs to zmq ws
    exists = any(isinstance(h, NodeIdZMQHandler) for h in logger.handlers)
    if not exists:
        handler = NodeIdZMQHandler(ip, port)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
    else:
        handler = next(h for h in logger.handlers if isinstance(h, NodeIdZMQHandler))

    handler.register_node_id(node_id=node_id)
