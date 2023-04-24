import time
import os
import pathlib

import dill
import multiprocessing as mp

import chimerapy as cp

from ..conftest import GenNode
from ..streams import AudioNode, VideoNode, ImageNode, TabularNode

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"


def test_run_node_in_debug_mode(logreceiver):

    data_nodes = [AudioNode, VideoNode, TabularNode, ImageNode]

    for i, node_cls in enumerate(data_nodes):
        n = node_cls(name=f"{i}", debug_port=logreceiver.port)
        logger.info("Running Node")
        n.run(blocking=False)
        logger.info("Outside of Node execution")

        time.sleep(2)

        logger.info("Shutting down Node")
        n.shutdown()


def test_create_node_and_run_in_process(logreceiver):

    n = VideoNode(name="Video", debug_port=logreceiver.port)

    # Adding shared variable that would be typically added by the Worker
    n._running = mp.Value("i", True)
    p = mp.Process(target=n.run)
    p.start()
    logger.info("Running Node")

    time.sleep(2)

    n.shutdown()
    logger.info("Shutting down Node")

    p.join()
    # p.terminate()


def test_create_multiple_nodes_after_pickling(logreceiver):

    ns = []
    for i in range(2):
        n = GenNode(name=f"G{i}")
        pkl_n = dill.dumps(n)
        nn = dill.loads(pkl_n)

        # Worker-injected information
        nn.debug_port = logreceiver.port
        nn._running = mp.Value("i", True)

        # Running
        logger.info("Running Node")
        p = mp.Process(target=nn.run)
        p.start()
        ns.append((p, nn))

    time.sleep(1)

    for p, n in ns:
        n.shutdown()
        p.join()
        logger.info("Shutting down Node")
        # assert n.exitcode == 0
