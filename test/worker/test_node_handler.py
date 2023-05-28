from ..conftest import linux_run_only
from ..streams.data_nodes import VideoNode, AudioNode, ImageNode, TabularNode
from ..networking.test_client_server import server

import time
from concurrent.futures import wait

import dill
import pytest
from pytest_lazyfixture import lazy_fixture
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()


# Constants
assert server
NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    "an": AudioNode,
}


@pytest.mark.parametrize("node", [lazy_fixture("gen_node"), lazy_fixture("con_node")])
def test_worker_create_node(worker, node):

    # Simple single node without connection
    msg = {
        "id": node.id,
        "pickled": dill.dumps(node),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    worker.create_node(msg).result(
        timeout=cp.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied creating nodes")
    assert node.id in worker.nodes


@linux_run_only
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
    worker.create_node(msg).result(
        timeout=cp.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied creating nodes")
    assert node.id in worker.nodes


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
    worker.create_node(msg).result(
        timeout=cp.config.get("worker.timeout.node-creation")
    )

    logger.debug("Start nodes!")
    worker.start_nodes().result(timeout=5)
    worker.record_nodes().result(timeout=5)

    logger.debug("Let nodes run for some time")
    time.sleep(2)

    # Stop the node
    worker.stop_nodes().result(timeout=5)
    data = worker.gather().result(timeout=5)
    assert isinstance(data, dict)


# @pytest.mark.repeat(5)
def test_worker_data_archiving(worker):

    # Just for debugging
    # worker.delete_temp = False

    nodes = []
    for node_name, node_class in NAME_CLASS_MAP.items():
        nodes.append(node_class(name=node_name))

    # Simple single node without connection
    futures = []
    for node in nodes:
        msg = {
            "id": node.id,
            "name": node.name,
            "pickled": dill.dumps(node),
            "in_bound": [],
            "in_bound_by_name": [],
            "out_bound": [],
            "follow": None,
        }
        futures.append(worker.create_node(msg))

    assert wait(futures, timeout=10)

    logger.debug("Start nodes!")
    worker.start_nodes().result(timeout=5)
    logger.debug("Let nodes run for some time")
    time.sleep(1)

    logger.debug("Recording nodes!")
    worker.record_nodes().result(timeout=5)
    logger.debug("Let nodes run for some time")
    time.sleep(1)

    worker.stop_nodes().result(timeout=5)

    worker.collect().result(timeout=30)

    for node_name in NAME_CLASS_MAP:
        assert (worker.tempfolder / node_name).exists()
