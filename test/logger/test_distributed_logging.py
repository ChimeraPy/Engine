import time

import chimerapy as cp
from ..conftest import TEST_DATA_DIR, GenNode, ConsumeNode
import glob
import pytest
from ..utils import uuid

pytestmark = [pytest.mark.slow]


def assert_has_log_with(file: str, text: str):
    """Assert that a file contains a log with the given text."""
    with open(file) as f:
        assert text in f.read()


def test_manager_ability_to_collect_logs():
    assert cp.config.get("manager.logs-sink.enabled") is False
    cp.config.set("manager.logs-sink.enabled", True)  # re-enable logs sink

    manager = cp.Manager(port=0, logdir=TEST_DATA_DIR)
    assert manager.logs_sink is not None

    worker_ids = []

    for j in range(5):
        worker = cp.Worker(name=f"worker_{j}", port=0, id=uuid())
        worker.connect(manager.host, manager.port)
        worker_ids.append(worker.id)
        time.sleep(5)

        worker.logger.info(f"Dummy log from worker {j}")

        worker.deregister().result(timeout=10)
        time.sleep(5)

        assert worker.id not in manager.logs_sink.handler.handlers
        worker.shutdown()

    manager.shutdown()

    assert len(worker_log_files := glob.glob(f"{manager.logdir}/*.log")) == 5

    for w_id, f in zip(worker_ids, worker_log_files):
        assert_has_log_with(f, "Dummy log from worker")


def test_manager_ability_to_collect_logs_with_worker_nodes():
    assert cp.config.get("manager.logs-sink.enabled") is False
    cp.config.set("manager.logs-sink.enabled", True)  # re-enable logs sink

    manager = cp.Manager(port=0, logdir=TEST_DATA_DIR)
    assert manager.logs_sink is not None

    gen_node = GenNode(name="Gen1")
    con_node = ConsumeNode(name="Con1")

    graph = cp.Graph()
    graph.add_nodes_from([gen_node, con_node])
    graph.add_edge(gen_node, con_node)

    worker1 = cp.Worker(name="worker1", port=0, id=uuid())
    worker1.connect(manager.host, manager.port)

    worker2 = cp.Worker(name="worker2", port=0, id=uuid())
    worker2.connect(manager.host, manager.port)

    worker_node_map = {worker1.id: [gen_node.id], worker2.id: [con_node.id]}

    manager.commit_graph(graph, worker_node_map).result(timeout=30)

    manager.start().result(timeout=5)
    time.sleep(3)
    manager.stop().result(timeout=5)

    manager.shutdown()
    worker1.shutdown()
    worker2.shutdown()

    time.sleep(2)

    assert len(glob.glob(f"{manager.logdir}/*.log")) == 2
