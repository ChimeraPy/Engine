# Built-in Import
from typing import List, Optional
import logging

# Internal Imports
from .logger import LOGGING_CONFIG


def debug(loggers: Optional[List[str]] = None):

    if type(loggers) == type(None):
        loggers = LOGGING_CONFIG["loggers"]

    assert loggers != None

    for logger_name in loggers:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
