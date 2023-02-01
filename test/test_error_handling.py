# Built-in Imports
import logging
import time
import dill
import pathlib
import os

# Third-party Imports
import yaml
import pytest

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()


class PlainNode(cp.Node):
    def step(self):
        return 1


class FailedToStartNode(cp.Node):
    def run(self):
        raise RuntimeError("Mock Error")


class FailedToConnectNode(cp.Node):
    def _prep(self):
        self.networking = False
        super()._prep()


class PrepInfiniteNode(cp.Node):
    def prep(self):
        while True:
            time.sleep(1)


class PrepErrorNode(cp.Node):
    def prep(self):
        raise RuntimeError("HA! GOTCHA!")


@pytest.mark.parametrize(
    "node_cls, expected_success",
    [
        (PlainNode, True),
        (FailedToStartNode, False),
        (FailedToConnectNode, False),
        (PrepInfiniteNode, False),
        (PrepErrorNode, False),
    ],
)
def test_faulty_node_creation_worker_only(worker, node_cls, expected_success):

    faulty_node = node_cls(name="test")

    # Simple single node without connection
    msg = {
        "node_name": faulty_node.name,
        "pickled": dill.dumps(faulty_node),
        "in_bound": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    actual_success = worker.create_node(msg)
    assert actual_success == expected_success


@pytest.mark.parametrize(
    "node_cls, expected_success",
    [
        (PlainNode, True),
        (FailedToStartNode, False),
        (FailedToConnectNode, False),
        (PrepInfiniteNode, False),
        (PrepErrorNode, False),
    ],
)
def test_faulty_node_creation_with_manager(manager, worker, node_cls, expected_success):

    # Create original containers
    simple_graph = cp.Graph()
    new_node = node_cls(name="test")
    simple_graph.add_nodes_from([new_node])
    mapping = {worker.name: [new_node.name]}

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Request node creation
    actual_success = manager.commit_graph(graph=simple_graph, mapping=mapping)
    assert actual_success == expected_success
