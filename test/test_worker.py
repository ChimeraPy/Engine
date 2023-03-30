import time
import os
import pathlib
from functools import partial
import requests

import dill

import chimerapy as cp

from .conftest import GenNode, ConsumeNode

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"


def test_worker_create_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "id": gen_node.id,
        "pickled": dill.dumps(gen_node),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Finishied creating nodes")
    assert gen_node.id in worker.nodes
    assert isinstance(worker.nodes_extra[gen_node.id]["node_object"], cp.Node)


def test_worker_create_unknown_node(worker):
    class UnknownNode(cp.Node):
        def step(self):
            return 2

    node = UnknownNode(name="Unk1")

    # Simple single node without connection
    msg = {
        "id": node.id,
        "pickled": dill.dumps(node),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }
    del UnknownNode

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Finishied creating nodes")
    assert node.id in worker.nodes
    assert isinstance(worker.nodes_extra[node.id]["node_object"], cp.Node)


def test_step_single_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "id": gen_node.id,
        "pickled": dill.dumps(gen_node),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Step through")
    worker.step()

    logger.debug("Let nodes run for some time")
    time.sleep(2)
    

def test_starting_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "id": gen_node.id,
        "pickled": dill.dumps(gen_node),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Waiting before starting!")
    time.sleep(2)

    logger.debug("Start nodes!")
    worker.start_nodes()

    logger.debug("Let nodes run for some time")
    time.sleep(2)

    r = requests.get(
        f"http://{worker.ip}:{worker.port}" + "/nodes/gather",
        timeout=cp.config.get("manager.timeout.info-request"),
    )
    assert r.status_code == requests.codes.ok
