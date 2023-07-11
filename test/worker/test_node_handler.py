from ..conftest import linux_run_only
from ..streams.data_nodes import VideoNode, AudioNode, ImageNode, TabularNode
from ..networking.test_client_server import server

import time
from concurrent.futures import wait

import pytest
import chimerapy_engine as cpe

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


# Constants
assert server
NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    "an": AudioNode,
}


@pytest.mark.parametrize("context", ["multiprocessing", "threading"])
def test_worker_create_node(worker, gen_node, context):

    logger.debug("Create nodes")
    worker.create_node(cpe.NodeConfig(gen_node, context=context)).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied creating nodes")
    assert gen_node.id in worker.nodes

    logger.debug("Destroy nodes")
    worker.destroy_node(gen_node.id).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied destroying nodes")
    assert gen_node.id not in worker.nodes


@pytest.mark.parametrize(
    "context_order",
    [["multiprocessing", "threading"], ["threading", "multiprocessing"]],
)
def test_worker_create_node_different_context(
    worker, gen_node, con_node, context_order
):

    logger.debug("Create nodes")
    worker.create_node(cpe.NodeConfig(gen_node, context=context_order[0])).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied creating nodes")
    assert gen_node.id in worker.nodes

    logger.debug("Destroy nodes")
    worker.destroy_node(gen_node.id).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    worker.create_node(cpe.NodeConfig(con_node, context=context_order[1])).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied creating nodes")
    assert con_node.id in worker.nodes

    worker.destroy_node(con_node.id).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied destroying nodes")
    assert gen_node.id not in worker.nodes
    assert con_node.id not in worker.nodes


@linux_run_only
def test_worker_create_unknown_node(worker):
    class UnknownNode(cpe.Node):
        def step(self):
            return 2

    node = UnknownNode(name="Unk1")

    # Simple single node without connection
    node_config = cpe.NodeConfig(node)
    del UnknownNode

    logger.debug("Create nodes")
    worker.create_node(node_config).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
    )

    logger.debug("Finishied creating nodes")
    assert node.id in worker.nodes


@pytest.mark.parametrize("context", ["multiprocessing", "threading"])
def test_starting_node(worker, gen_node, context):

    logger.debug("Create nodes")
    worker.create_node(cpe.NodeConfig(gen_node, context=context)).result(
        timeout=cpe.config.get("worker.timeout.node-creation")
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


@pytest.mark.parametrize("context", ["multiprocessing", "threading"])
def test_worker_data_archiving(worker, context):

    # Just for debugging
    # worker.delete_temp = False

    nodes = []
    for node_name, node_class in NAME_CLASS_MAP.items():
        nodes.append(node_class(name=node_name))

    # Simple single node without connection
    futures = []
    for node in nodes:
        futures.append(worker.create_node(cpe.NodeConfig(node, context=context)))

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
