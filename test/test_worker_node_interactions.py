import time
import logging
import threading
import multiprocessing as mp
import queue
import sys
import os
import pathlib
from functools import partial
import requests

import pytest
import dill

import chimerapy as cp

from .conftest import GenNode, ConsumeNode, linux_expected_only, linux_run_only
from pytest_lazyfixture import lazy_fixture
from .streams import AudioNode, VideoNode, ImageNode, TabularNode

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"


def target_function(q):
    time.sleep(1)
    q.put(1)


def test_multiple_fork_process():

    NUM = 10
    ps = []
    q = mp.Queue()
    for i in range(NUM):
        p = mp.Process(target=target_function, args=(q,))
        p.start()
        ps.append(p)

    for p in ps:
        p.join(5)
        p.terminate()

    assert q.qsize() == NUM


def test_multiple_threads():

    NUM = 100
    ts = []
    q = queue.Queue()
    for i in range(NUM):
        t = threading.Thread(target=target_function, args=(q,))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    assert q.qsize() == NUM


def test_run_node_in_debug_mode(logreceiver):

    data_nodes = [AudioNode, VideoNode, TabularNode, ImageNode]

    for i, node_cls in enumerate(data_nodes):
        n = node_cls(name=f"{i}", debug="step")

        for i in range(5):
            n.step()

        n.shutdown()


def test_create_multiple_nodes_not_pickled(logreceiver):

    ns = []
    for i in range(3):
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
    for i in range(3):
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


def test_create_multiple_workers():

    workers = []

    for i in range(5):
        worker = cp.Worker(name=f"{i}", port=0)
        workers.append(worker)

    time.sleep(5)

    for worker in workers:
        worker.shutdown()


# @pytest.mark.repeat(3)
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


@linux_expected_only
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


def test_worker_create_multiple_node(worker):

    to_be_created_nodes = []
    for i in range(10):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        to_be_created_nodes.append(new_node.name)

        # Simple single node without connection
        msg = {
            "id": new_node.id,
            "pickled": dill.dumps(new_node),
            "in_bound": [],
            "in_bound_by_name": [],
            "out_bound": [],
            "follow": None,
        }

        try:
            worker.create_node(msg)
        except OSError:
            continue


def test_worker_create_multiple_nodes_stress(worker):

    to_be_created_nodes = []
    for i in range(2):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        new_node2 = ConsumeNode(name=f"Con{i}")
        to_be_created_nodes.append(new_node.id)
        to_be_created_nodes.append(new_node2.id)

        # Simple single node without connection
        msg = {
            "id": new_node.id,
            "pickled": dill.dumps(new_node),
            "in_bound": [],
            "in_bound_by_name": [],
            "out_bound": [],
            "follow": None,
        }

        msg2 = {
            "id": new_node2.id,
            "pickled": dill.dumps(new_node2),
            "in_bound": [],
            "in_bound_by_name": [],
            "out_bound": [],
            "follow": None,
        }

        try:
            worker.create_node(msg)
            worker.create_node(msg2)
        except OSError:
            continue

    # Confirm that the node was created
    for node_id in to_be_created_nodes:
        assert node_id in worker.nodes


# @linux_expected_only
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


def test_two_nodes_connect(worker, gen_node, con_node):

    # Simple single node without connection
    msg = {
        "id": gen_node.id,
        "pickled": dill.dumps(gen_node),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [con_node.name],
        "follow": None,
    }

    # Simple single node without connection
    msg2 = {
        "id": con_node.id,
        "pickled": dill.dumps(con_node),
        "in_bound": [gen_node.name],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    worker.create_node(msg)
    worker.create_node(msg2)

    node_server_data = worker.create_node_server_data()
    logger.debug(f"Send the server data: {node_server_data}")
    worker.exec_coro(
        partial(worker.process_node_server_data, {"data": node_server_data["nodes"]})
    )
    time.sleep(3)


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
    time.sleep(5)


def test_worker_gather(worker, gen_node):

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
    time.sleep(5)

    r = requests.get(
        f"http://{worker.ip}:{worker.port}" + "/nodes/gather",
        timeout=cp.config.get("manager.timeout.info-request"),
    )
    assert r.status_code == requests.codes.ok


@pytest.mark.slow
@pytest.mark.timeout(600)
@pytest.mark.parametrize(
    "_manager,_worker",
    [
        (lazy_fixture("manager"), lazy_fixture("worker")),
        pytest.param(
            lazy_fixture("manager"),
            lazy_fixture("dockered_worker"),
            marks=linux_run_only,
        ),
    ],
)
def test_manager_directing_worker_to_create_node(_manager, _worker):

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name=f"Gen1")
    simple_graph.add_nodes_from([new_node])
    mapping = {_worker.id: [new_node.id]}

    # Connect to the manager
    _worker.connect(host=_manager.host, port=_manager.port)

    # Then register graph to Manager
    _manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    _manager.map_graph(mapping)

    # Request node creation
    assert _manager.request_node_creation(worker_id=_worker.id, node_id=new_node.id)
    assert new_node.id in _manager.workers[_worker.id].nodes


@pytest.mark.slow
@pytest.mark.timeout(600)
@pytest.mark.parametrize(
    "_manager,_worker",
    [
        (lazy_fixture("manager"), lazy_fixture("worker")),
        pytest.param(
            lazy_fixture("manager"),
            lazy_fixture("dockered_worker"),
            marks=linux_run_only,
        ),
    ],
)
def test_stress_manager_directing_worker_to_create_node(_manager, _worker):

    # Create original containers
    simple_graph = cp.Graph()
    to_be_created_nodes = []
    mapping = {_worker.id: []}

    for i in range(2):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        new_node2 = ConsumeNode(name=f"Con{i}")
        to_be_created_nodes.append(new_node.id)
        to_be_created_nodes.append(new_node2.id)

        # Simple single node without connection
        simple_graph.add_nodes_from([new_node, new_node2])

        # Create mapping
        mapping[_worker.id].append(new_node.id)
        mapping[_worker.id].append(new_node2.id)

    # Connect to the manager
    _worker.connect(host=_manager.host, port=_manager.port)

    # Then register graph to Manager
    _manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    _manager.map_graph(mapping)

    # Request node creation
    for node_id in to_be_created_nodes:
        assert _manager.request_node_creation(worker_id=_worker.id, node_id=node_id)
        assert node_id in _manager.workers[_worker.id].nodes
