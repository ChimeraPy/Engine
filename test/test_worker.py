from .conftest import linux_run_only, TEST_DATA_DIR, TEST_SAMPLE_DATA_DIR
from .streams.data_nodes import VideoNode, AudioNode, ImageNode, TabularNode
from .networking.test_client_server import server

import time
import os
import requests
import shutil
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
    assert isinstance(worker.nodes_extra[node.id]["node_object"], cp.Node)


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
    assert isinstance(worker.nodes_extra[node.id]["node_object"], cp.Node)


# def test_step_single_node(worker, gen_node):

#     # Simple single node without connection
#     msg = {
#         "id": gen_node.id,
#         "pickled": dill.dumps(gen_node),
#         "in_bound": [],
#         "in_bound_by_name": [],
#         "out_bound": [],
#         "follow": None,
#     }

#     logger.debug("Create nodes")
#     worker.create_node(msg).result(
#         timeout=cp.config.get("worker.timeout.node-creation")
#     )

#     logger.debug("Step through")
#     worker.step().result(timeout=5)

#     time.sleep(2)


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

    new_folder_name = dst / f"{worker.name}-{worker.id}"
    if new_folder_name.exists():
        shutil.rmtree(new_folder_name)

    future = worker._exec_coro(worker._async_send_archive_locally(dst))
    assert future.result(timeout=5)

    dst_worker = dst / f"{worker.name}-{worker.id}"
    dst_test_file = dst_worker / "test.txt"
    assert dst_worker.exists() and dst_test_file.exists()

    with open(dst_test_file, "r") as f:
        assert f.read() == "hello"


def test_send_archive_remotely(worker, server):

    # Make a copy of example logs
    shutil.copytree(
        str(TEST_SAMPLE_DATA_DIR / "chimerapy_logs"), str(worker.tempfolder / "data")
    )

    future = worker._exec_coro(
        worker._async_send_archive_remotely(server.host, server.port)
    )
    # future.result(timeout=10)
    future.result()
    logger.debug(server.file_transfer_records)

    # Also check that the file exists
    for record in server.file_transfer_records[worker.id].values():
        assert record["dst_filepath"].exists()
