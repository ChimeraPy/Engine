import time
import logging
import threading
import multiprocessing as mp
import queue
import sys

import pytest
import dill

import chimerapy as cp

from .conftest import GenNode, ConsumeNode, linux_expected_only, linux_run_only
from pytest_lazyfixture import lazy_fixture

logger = logging.getLogger("chimerapy")


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
        p.join()

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


# @linux_expected_only
def test_create_multiple_nodes(logreceiver):

    ns = []
    for i in range(3):
        n = GenNode(name=f"G{i}")
        n.config("0.0.0.0", 9000, [], [], networking=False)
        n.start()
        ns.append(n)

    time.sleep(1)

    for n in ns:
        n.shutdown()
        n.join()
        assert n.exitcode == 0


# @linux_expected_only
def test_create_multiple_nodes_after_pickling():

    ns = []
    for i in range(10):
        n = GenNode(name=f"G{i}")
        pkl_n = dill.dumps(n)
        nn = dill.loads(pkl_n)
        nn.config("0.0.0.0", 9000, [], [], networking=False)
        nn.start()
        ns.append(nn)

    for n in ns:
        n.shutdown()
        n.join()
        assert n.exitcode == 0


def test_create_multiple_workers():

    workers = []

    for i in range(10):
        worker = cp.Worker(name=f"{i}")
        workers.append(worker)

    time.sleep(5)

    for worker in workers:
        worker.shutdown()


# @linux_expected_only
@pytest.mark.repeat(3)
def test_worker_create_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "pickled": dill.dumps(gen_node),
            "in_bound": [],
            "out_bound": [],
        }
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Finishied creating nodes")
    assert gen_node.name in worker.nodes
    assert isinstance(worker.nodes[gen_node.name]["node_object"], cp.Node)


@linux_expected_only
def test_worker_create_unknown_node(worker):
    class UnknownNode(cp.Node):
        def step(self):
            return 2

    node = UnknownNode(name="Unk1")

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": node.name,
            "pickled": dill.dumps(node),
            "in_bound": [],
            "out_bound": [],
        }
    }
    del UnknownNode

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Finishied creating nodes")
    assert node.name in worker.nodes
    assert isinstance(worker.nodes[node.name]["node_object"], cp.Node)


# @linux_expected_only
def test_worker_create_nodes(worker):

    to_be_created_nodes = []
    for i in range(10):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        to_be_created_nodes.append(new_node.name)

        # Simple single node without connection
        msg = {
            "data": {
                "node_name": new_node.name,
                "pickled": dill.dumps(new_node),
                "in_bound": [],
                "out_bound": [],
            }
        }

        try:
            worker.create_node(msg)
        except OSError:
            continue


# @linux_expected_only
@pytest.mark.repeat(10)
def test_worker_create_multiple_nodes_stress(worker):

    to_be_created_nodes = []
    for i in range(5):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        new_node2 = ConsumeNode(name=f"Con{i}")
        to_be_created_nodes.append(new_node.name)
        to_be_created_nodes.append(new_node2.name)

        # Simple single node without connection
        msg = {
            "data": {
                "node_name": new_node.name,
                "pickled": dill.dumps(new_node),
                "in_bound": [],
                "out_bound": [],
            }
        }

        msg2 = {
            "data": {
                "node_name": new_node2.name,
                "pickled": dill.dumps(new_node2),
                "in_bound": [],
                "out_bound": [],
            }
        }

        try:
            worker.create_node(msg)
            worker.create_node(msg2)
        except OSError:
            continue

    # Confirm that the node was created
    for node_name in to_be_created_nodes:
        assert node_name in worker.nodes


# @linux_expected_only
def test_step_single_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "pickled": dill.dumps(gen_node),
            "in_bound": [],
            "out_bound": [],
        }
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Step through")
    worker.step({})

    logger.debug("Let nodes run for some time")
    time.sleep(2)


# @linux_expected_only
def test_two_nodes_connect(worker, gen_node, con_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "pickled": dill.dumps(gen_node),
            "in_bound": [],
            "out_bound": [con_node.name],
        }
    }

    # Simple single node without connection
    msg2 = {
        "data": {
            "node_name": con_node.name,
            "pickled": dill.dumps(con_node),
            "in_bound": [gen_node.name],
            "out_bound": [],
        }
    }

    logger.debug("Create nodes")
    worker.create_node(msg)
    worker.create_node(msg2)

    node_server_data = worker.create_node_server_data()
    logger.debug(f"Send the server data: {node_server_data}")
    worker.process_node_server_data({"data": node_server_data["nodes"]})


# @linux_expected_only
def test_starting_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "pickled": dill.dumps(gen_node),
            "in_bound": [],
            "out_bound": [],
        }
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Waiting!")
    time.sleep(2)

    logger.debug("Start nodes!")
    worker.start_nodes({})

    logger.debug("Let nodes run for some time")
    time.sleep(2)


# @linux_expected_only
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
    mapping = {_worker.name: [new_node.name]}

    # Connect to the manager
    _worker.connect(host=_manager.host, port=_manager.port)

    # Then register graph to Manager
    _manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    _manager.map_graph(mapping)

    # Request node creation
    _manager.request_node_creation(worker_name=_worker.name, node_name="Gen1")
    _manager.wait_until_node_creation_complete(
        worker_name=_worker.name, node_name="Gen1"
    )


# @linux_expected_only
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
    mapping = {_worker.name: []}

    for i in range(5):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        new_node2 = ConsumeNode(name=f"Con{i}")
        to_be_created_nodes.append(new_node.name)
        to_be_created_nodes.append(new_node2.name)

        # Simple single node without connection
        simple_graph.add_nodes_from([new_node, new_node2])

        # Create mapping
        mapping[_worker.name].append(new_node.name)
        mapping[_worker.name].append(new_node2.name)

    # Connect to the manager
    _worker.connect(host=_manager.host, port=_manager.port)

    # Then register graph to Manager
    _manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    _manager.map_graph(mapping)

    # Request node creation
    for node_name in to_be_created_nodes:
        _manager.request_node_creation(worker_name=_worker.name, node_name=node_name)
        _manager.wait_until_node_creation_complete(
            worker_name=_worker.name, node_name=node_name
        )
