from typing import Dict, Any
import time
import logging
import pdb

import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")


class LowFrequencyNode(cp.Node):
    def prep(self):
        self.i = 0

    def step(self):
        if self.i == 0:
            time.sleep(0.5)
            self.i += 1
            return self.i
        else:
            time.sleep(3)
            self.i += 1
            return self.i


class HighFrequencyNode(cp.Node):
    def prep(self):
        self.i = 0

    def step(self):
        time.sleep(0.1)
        self.i += 1
        return self.i


class SubsequentNode(cp.Node):
    def prep(self):
        self.record = {}

    def step(self, data: Dict[str, Any]):

        for k, v in data.items():
            self.record[k] = v

        return self.record


@pytest.fixture
def step_up_graph():
    hf = HighFrequencyNode(name="hf")
    lf = LowFrequencyNode(name="lf")
    sn = SubsequentNode(name="sn")

    graph = cp.Graph()
    graph.add_nodes_from([hf, lf, sn])

    graph.add_edge(src=hf, dst=sn, follow=True)
    graph.add_edge(src=lf, dst=sn)

    return graph


@pytest.fixture
def step_down_graph():
    hf = HighFrequencyNode(name="hf")
    lf = LowFrequencyNode(name="lf")
    sn = SubsequentNode(name="sn")

    graph = cp.Graph()
    graph.add_nodes_from([hf, lf, sn])

    graph.add_edge(src=hf, dst=sn)
    graph.add_edge(src=lf, dst=sn, follow=True)

    return graph


@pytest.mark.parametrize(
    "config_graph, follow",
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
def test_node_frequency_execution(manager, worker, config_graph, follow):

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to manager
    manager.commit_graph(
        config_graph,
        {
            "local": ["hf", "lf", "sn"],
        },
    )

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(3)
    manager.stop()

    # Then request gather and confirm that the data is valid
    latest_data_values = manager.gather()
    logger.info(f"Data Values: {latest_data_values}")
    step_records = latest_data_values["sn"]

    if follow == "up":
        assert step_records["lf"] == latest_data_values["lf"]
        assert np.abs((step_records["hf"] - latest_data_values["hf"])) < 5
    elif follow == "down":
        assert step_records["lf"] == latest_data_values["lf"]
        assert step_records["hf"] != latest_data_values["hf"]
