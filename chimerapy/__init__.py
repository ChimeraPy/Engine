# Setup the logging for the library
# References:
# https://docs.python-guide.org/writing/logging/
# https://stackoverflow.com/questions/13649664/how-to-use-logging-with-pythons-fileconfig-and-configure-the-logfile-filename
import logging.config

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
        "datagram": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.handlers.DatagramHandler",
            "host": "127.0.0.1",
            "port": 5555,
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True,
        },
        "subprocess": {"handlers": ["datagram"], "level": "DEBUG", "propagate": True},
    },
}

# Setup the logging configuration
logging.config.dictConfig(LOGGING_CONFIG)

# Interal Imports
from .manager import Manager
from .node import Node
from .worker import Worker
from .graph import Graph

from .client import Client
from .server import Server
from .enums import *
from .utils import log

# Then define the entry points
from . import entry
