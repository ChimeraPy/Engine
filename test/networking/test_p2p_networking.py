import time

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp
from ..conftest import linux_run_only
from ..mock import DockeredWorker

logger = cp._logger.getLogger("chimerapy")
cp.debug()


# @pytest.mark.repeat(10)
@pytest.mark.parametrize(
    "config_manager, expected_worker_to_nodes",
    [
        (lazy_fixture("single_node_no_connections_manager"), {"local": ["Gen1"]}),
        (
            lazy_fixture("multiple_nodes_one_worker_manager"),
            {"local": ["Gen1", "Con1"]},
        ),
        (
            lazy_fixture("multiple_nodes_multiple_workers_manager"),
            {"local": ["Gen1"], "local2": ["Con1"]},
        ),
        pytest.param(
            lazy_fixture("dockered_single_node_no_connections_manager"),
            {"test": ["Gen1"]},
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
            {"test": ["Gen1", "Con1"]},
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
            {"local": ["Gen1"], "local2": ["Con1"]},
            marks=linux_run_only,
        ),
    ],
)
def test_detecting_when_all_nodes_are_ready(config_manager, expected_worker_to_nodes):

    # Extract all the nodes
    nodes_names = []
    for worker_name in config_manager.workers:

        # Assert that the worker has their expected nodes
        expected_nodes = expected_worker_to_nodes[worker_name]
        union = set(expected_nodes) | set(
            config_manager.workers[worker_name]["nodes_status"].keys()
        )
        assert len(union) == len(expected_nodes)

        # Assert that all the nodes should be INIT
        for node_name in config_manager.workers[worker_name]["nodes_status"]:
            assert config_manager.workers[worker_name]["nodes_status"][node_name][
                "INIT"
            ]
            nodes_names.append(node_name)

    logger.debug(f"After creation of p2p network workers: {config_manager.workers}")

    # The config manager should have all the nodes are registered
    assert all([config_manager.graph.has_node_by_name(x) for x in nodes_names])

    # Extract all the nodes
    nodes_names = []
    for worker_name in config_manager.workers:
        for node_name in config_manager.workers[worker_name]["nodes_status"]:
            assert config_manager.workers[worker_name]["nodes_status"][node_name][
                "CONNECTED"
            ]
            nodes_names.append(node_name)

    # The config manager should have all the nodes registered as CONNECTED
    assert all([x in config_manager.nodes_server_table for x in nodes_names])

    # Extract all the nodes
    nodes_names = []
    for worker_name in config_manager.workers:
        for node_name in config_manager.workers[worker_name]["nodes_status"]:
            assert config_manager.workers[worker_name]["nodes_status"][node_name][
                "READY"
            ]
            nodes_names.append(node_name)

    # The config manager should have all the nodes registered as READY
    assert all([x in config_manager.nodes_server_table for x in nodes_names])


# @pytest.mark.repeat(10)
@pytest.mark.parametrize(
    "config_manager,expected_output",
    [
        (lazy_fixture("single_node_no_connections_manager"), {"Gen1": 2}),
        (lazy_fixture("multiple_nodes_one_worker_manager"), {"Gen1": 2, "Con1": 6}),
        (
            lazy_fixture("multiple_nodes_multiple_workers_manager"),
            {"Gen1": 2, "Con1": 6},
        ),
        (lazy_fixture("slow_single_node_single_worker_manager"), {"Slo1": 5}),
        pytest.param(
            lazy_fixture("dockered_single_node_no_connections_manager"),
            {"Gen1": 2},
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=linux_run_only,
        ),
    ],
)
def test_manager_single_step_after_commit_graph(config_manager, expected_output):

    # Take a single step and see if the system crashes and burns
    config_manager.step()
    time.sleep(5)

    # Then request gather and confirm that the data is valid
    latest_data_values = config_manager.gather()

    # Assert
    for k, v in expected_output.items():
        assert (
            k in latest_data_values
            and isinstance(latest_data_values[k], cp.DataChunk)
            and latest_data_values[k].get("default")["value"] == v
        )


@pytest.mark.parametrize(
    "config_manager,expected_output",
    [
        (lazy_fixture("single_node_no_connections_manager"), {"Gen1": 2}),
        (lazy_fixture("multiple_nodes_one_worker_manager"), {"Gen1": 2, "Con1": 6}),
        (
            lazy_fixture("multiple_nodes_multiple_workers_manager"),
            {"Gen1": 2, "Con1": 6},
        ),
        (lazy_fixture("slow_single_node_single_worker_manager"), {"Slo1": 5}),
        pytest.param(
            lazy_fixture("dockered_single_node_no_connections_manager"),
            {"Gen1": 2},
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=linux_run_only,
        ),
    ],
)
def test_manager_start(config_manager, expected_output):

    # Take a single step and see if the system crashes and burns
    config_manager.start()
    time.sleep(5)
    config_manager.stop()

    # Then request gather and confirm that the data is valid
    latest_data_values = config_manager.gather()

    # Assert
    for k, v in expected_output.items():
        assert (
            k in latest_data_values
            and isinstance(latest_data_values[k], cp.DataChunk)
            and latest_data_values[k].get("default")["value"] == v
        )
