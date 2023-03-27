import time
import logging
import os
import pathlib

import dill
import pytest
import numpy as np
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp

from .conftest import GenNode, SubsequentNode, HighFrequencyNode, LowFrequencyNode
from .streams import AudioNode, VideoNode, ImageNode, TabularNode

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"


def target_function(q):
    time.sleep(1)
    q.put(1)


def test_run_node_in_debug_mode(logreceiver):

    data_nodes = [AudioNode, VideoNode, TabularNode, ImageNode]

    for i, node_cls in enumerate(data_nodes):
        n = node_cls(name=f"{i}", debug="step")

        for i in range(5):
            n.step()

        n.shutdown()


def test_create_multiple_nodes_not_pickled(logreceiver):

    ns = []
    for i in range(2):
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
    for i in range(2):
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


@pytest.fixture
def step_up_graph():
    hf = HighFrequencyNode(name="hf")
    lf = LowFrequencyNode(name="lf")
    sn = SubsequentNode(name="sn")

    graph = cp.Graph()
    graph.add_nodes_from([hf, lf, sn])

    graph.add_edge(src=hf, dst=sn, follow=True)
    graph.add_edge(src=lf, dst=sn)

    return (graph, [hf.id, lf.id, sn.id])


@pytest.fixture
def step_down_graph():
    hf = HighFrequencyNode(name="hf")
    lf = LowFrequencyNode(name="lf")
    sn = SubsequentNode(name="sn")

    graph = cp.Graph()
    graph.add_nodes_from([hf, lf, sn])

    graph.add_edge(src=hf, dst=sn)
    graph.add_edge(src=lf, dst=sn, follow=True)

    return (graph, [hf.id, lf.id, sn.id])


@pytest.mark.parametrize(
    "config_graph_data, follow",
    [
        (
            lazy_fixture("step_up_graph"),
            "up",
        ),
        (
            lazy_fixture("step_down_graph"),
            "down",
        ),
    ],
)
def test_node_frequency_execution(manager, worker, config_graph_data, follow):

    # Decompose graph data
    config_graph, node_ids = config_graph_data

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to manager
    manager.commit_graph(
        config_graph,
        {
            worker.id: node_ids,
        },
    )

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(3)
    manager.stop()

    # Convert name to id
    name_to_id = {
        data["object"].name: n for n, data in manager.graph.G.nodes(data=True)
    }

    # Then request gather and confirm that the data is valid
    latest_data_values = manager.gather()
    logger.info(f"Data Values: {latest_data_values}")
    data_chunk_step_records = latest_data_values[name_to_id["sn"]]
    step_records = data_chunk_step_records.get("record")["value"]

    if follow == "up":
        assert step_records["lf"] == latest_data_values[name_to_id["lf"]]
        assert (
            np.abs(
                (
                    step_records["hf"].get("i")["value"]
                    - latest_data_values[name_to_id["hf"]].get("i")["value"]
                )
            )
            < 5
        )
    elif follow == "down":
        assert step_records["lf"] == latest_data_values[name_to_id["lf"]]
        assert step_records["hf"] != latest_data_values[name_to_id["hf"]]
