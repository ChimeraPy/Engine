import logging

import pytest
import glob
import random

from chimerapy.engine.logger.common import HandlerFactory
import chimerapy.engine as cpe

from ..utils import cleanup_and_recreate_dir, uuid
from ..conftest import TEST_DATA_DIR

pytestmark = [pytest.mark.unit]


def test_handler_factory_get_console_handler():
    hdlr = HandlerFactory.get("console", level=logging.DEBUG)
    assert hdlr.level == 10
    assert hdlr.formatter._fmt == "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    assert hdlr.formatter.datefmt == "%Y-%m-%d %H:%M:%S"

    hdlr = HandlerFactory.get("console", level=logging.INFO)
    assert hdlr.level == 20
    assert hdlr.formatter._fmt == "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    assert hdlr.formatter.datefmt == "%Y-%m-%d %H:%M:%S"


def test_handler_factory_get_node_id_context_console_handler():
    hdlr = HandlerFactory.get("console-node_id", level=logging.DEBUG)
    assert hdlr.level == 10
    assert (
        hdlr.formatter._fmt
        == "%(asctime)s [%(levelname)s] %(name)s(NodeID-[%(node_id)s]): %(message)s"
    )
    assert hdlr.formatter.datefmt == "%Y-%m-%d %H:%M:%S"

    hdlr = HandlerFactory.get("console-node_id", level=logging.INFO)
    assert hdlr.level == 20
    assert (
        hdlr.formatter._fmt
        == "%(asctime)s [%(levelname)s] %(name)s(NodeID-[%(node_id)s]): %(message)s"
    )
    assert hdlr.formatter.datefmt == "%Y-%m-%d %H:%M:%S"


def test_multiplexed_file_handler():
    loggers = {uuid(): logging.getLogger(f"test{j}") for j in range(10)}

    logs_sink = HandlerFactory.get(name="multiplexed-rotating-file", max_bytes=3000)

    for entity_id, logger in loggers.items():
        logger.setLevel(logging.DEBUG)
        cpe._logger.add_identifier_filter(logger, entity_id)
        logger.addHandler(logs_sink)

    logs_dir = (TEST_DATA_DIR / "distributed_logs").resolve()

    cleanup_and_recreate_dir(logs_dir)

    for j, entity_id in enumerate(loggers.keys()):
        logs_sink.initialize_entity(f"my_test_logger_{j}", entity_id, logs_dir)

    all_loggers = list(loggers.values())
    for j in range(1000):
        random.choice(all_loggers).info(f"Dummy log {j}")

    list(
        map(lambda x: logs_sink.deregister_entity(x), loggers.keys())
    )  # deregister all entities

    for j in range(10):
        assert (
            len(glob.glob(f"{logs_dir}/my_test_logger_{j}*.log.*")) >= 1
        )  # more than one file because of rotation

    cleanup_and_recreate_dir(logs_dir)
