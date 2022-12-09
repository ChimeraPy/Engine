# Built-in Import
from typing import List, Optional
import logging
import os

# Internal Imports
from ._logger import LOGGING_CONFIG


def debug(loggers: Optional[List[str]] = None):

    # Not provided, then get all
    if type(loggers) == type(None):
        loggers = [x for x in LOGGING_CONFIG["loggers"]]

    assert loggers != None

    # Change env variable and configurations
    os.environ["CHIMERAPY_DEBUG_LOGGERS"] = os.pathsep.join(loggers)
    for logger_name in loggers:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
