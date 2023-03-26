import logging

import pytest

from chimerapy.logger.common import HandlerFactory

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
