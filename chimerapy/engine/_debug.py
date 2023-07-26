# Built-in Import
from typing import List, Optional, Dict
import logging
import os

# Internal Imports
from ._logger import LOGGING_CONFIG


def debug(loggers: Optional[List[str]] = None):

    # Not provided, then get all
    if type(loggers) == type(None):
        logger_config: Dict[str, Dict] = LOGGING_CONFIG["loggers"]
        loggers = [x for x in logger_config]

    assert loggers is not None

    # Change env variable and configurations
    os.environ["CHIMERAPY_ENGINE_DEBUG_LOGGERS"] = os.pathsep.join(loggers)
    for logger_name in loggers:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
