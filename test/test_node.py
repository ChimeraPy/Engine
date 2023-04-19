import time
import logging
import os
import pathlib

import dill
import pytest
import numpy as np
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp

from .conftest import GenNode, SubsequentNode, HighFrequencyNode, LowFrequencyNode
from .streams import AudioNode, VideoNode, ImageNode, TabularNode

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"


def target_function(q):
    time.sleep(1)
    q.put(1)


def test_run_node_in_debug_mode(logreceiver):

    data_nodes = [AudioNode, VideoNode, TabularNode, ImageNode]

    for i, node_cls in enumerate(data_nodes):
        n = node_cls(name=f"{i}", debug="step")

        for i in range(5):
            n.step()

        n.shutdown()


def test_create_multiple_nodes_not_pickled(logreceiver):

    ns = []
    for i in range(2):
        n = VideoNode(name=f"G{i}", debug="stream", debug_port=logreceiver.port)
        n.start()
        ns.append(n)

    time.sleep(1)

    for n in ns:
        n.shutdown()
        n.join(5)
        n.terminate()
        # assert n.exitcode == 0


def test_create_multiple_nodes_after_pickling(logreceiver):

    ns = []
    for i in range(2):
        n = GenNode(name=f"G{i}")
        pkl_n = dill.dumps(n)
        nn = dill.loads(pkl_n)
        nn.config(
            "0.0.0.0",
            9000,
            TEST_DATA_DIR,
            [],
            [],
            ["Con1"],
            follow=None,
            networking=False,
            logging_level=logging.DEBUG,
            worker_logging_port=logreceiver.port,
        )
        nn.start()
        ns.append(nn)

    time.sleep(1)

    for n in ns:
        n.shutdown()
        n.join(5)
        n.terminate()
        # assert n.exitcode == 0
