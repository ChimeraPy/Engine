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

from .streams.data_nodes import VideoNode, AudioNode, ImageNode, TabularNode

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"

NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    "an": AudioNode,
}


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


def test_worker_data_archiving(worker):

    # Just for debugging
    # worker.delete_temp = False

    nodes = []
    for node_name, node_class in NAME_CLASS_MAP.items():
        nodes.append(node_class(name=node_name))

    # Simple single node without connection
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
        worker.create_node(msg)

    logger.debug("Waiting!")
    time.sleep(2)

    logger.debug("Start nodes!")
    worker.start_nodes()

    logger.debug("Let nodes run for some time")
    time.sleep(1)

    for node_name in NAME_CLASS_MAP:
        assert (worker.tempfolder / node_name).exists()


def test_send_archive_locally(worker):

    # Adding simple file
    test_file = worker.tempfolder / "test.txt"
    if test_file.exists():
        os.remove(test_file)
    else:
        with open(test_file, "w") as f:
            f.write("hello")

    dst = TEST_DATA_DIR / "test_folder"
    os.makedirs(dst, exist_ok=True)
    future = worker.exec_coro(worker._send_archive_locally(dst))
    future.result(timeout=5)

    dst_worker = dst / f"{worker.name}-{worker.id}"
    dst_test_file = dst_worker / "test.txt"
    assert dst_worker.exists() and dst_test_file.exists()

    with open(dst_test_file, "r") as f:
        assert f.read() == "hello"


def test_send_archive_remotely(worker, server):

    # Adding simple file
    test_file = worker.tempfolder / "test.txt"
    if test_file.exists():
        os.remove(test_file)
    else:
        with open(test_file, "w") as f:
            f.write("hello")

    future = worker.exec_coro(worker._send_archive_remotely(server.host, server.port))
    future.result(timeout=10)
    logger.debug(server.file_transfer_records)

    # Also check that the file exists
    for record in server.file_transfer_records[worker.id].values():
        assert record["dst_filepath"].exists()
