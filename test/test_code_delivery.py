import sys
import pathlib
import os
import pdb

import pytest
from pytest_lazyfixture import lazy_fixture
import chimerapy as cp

# Internal Imports
from .mock import test_package as tp
from .conftest import linux_run_only

# Constant
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent


@pytest.fixture
def local_node_graph(gen_node):
    graph = cp.Graph()
    graph.add_node(gen_node)
    return graph


@pytest.fixture
def packaged_node_graph():
    node = tp.TestNode(name="test")
    graph = cp.Graph()
    graph.add_node(node)
    return graph


@linux_run_only
@pytest.mark.parametrize(
    "config_graph",
    [
        (lazy_fixture("local_node_graph")),
        (lazy_fixture("packaged_node_graph")),
    ],
)
def test_sending_package(manager, dockered_worker, config_graph):

    dockered_worker.connect(host=manager.host, port=manager.port)

    manager.commit_graph(
        graph=config_graph,
        mapping={dockered_worker.name: list(config_graph.G.nodes())},
        timeout=10,
        send_packages=[
            {"name": "test_package", "path": TEST_DIR / "mock" / "test_package"}
        ],
    )

    for node_name in config_graph.G.nodes():
        assert (
            manager.workers[dockered_worker.name]["nodes_status"][node_name]["INIT"]
            == 1
        )
