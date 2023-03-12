import time

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp
from ..conftest import linux_run_only
from ..mock import DockeredWorker

logger = cp._logger.getLogger("chimerapy")
cp.debug()


# @pytest.mark.repeat(10)
# @pytest.mark.parametrize(
#     "config_manager",
#     [
#         (lazy_fixture("single_node_no_connections_manager")),
#         (lazy_fixture("multiple_nodes_one_worker_manager")),
#         (lazy_fixture("multiple_nodes_multiple_workers_manager")),
#         pytest.param(
#             lazy_fixture("dockered_single_node_no_connections_manager"),
#             marks=linux_run_only,
#         ),
#         pytest.param(
#             lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
#             marks=linux_run_only,
#         ),
#         pytest.param(
#             lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
#             marks=linux_run_only,
#         ),
#     ],
# )
# def test_detecting_when_all_nodes_are_ready(config_manager):
#
#     worker_node_map = config_manager.worker_graph_map
#
#     nodes_ids = []
#     for worker_id in config_manager.workers:
#
#         # Assert that the worker has their expected nodes
#         expected_nodes = worker_node_map[worker_id]
#         union = set(expected_nodes) | set(
#             config_manager.workers[worker_id].nodes.keys()
#         )
#         assert len(union) == len(expected_nodes)
#
#         # Assert that all the nodes should be INIT
#         for node_id in config_manager.workers[worker_id].nodes:
#             assert config_manager.workers[worker_id].nodes[node_id].init
#             nodes_ids.append(node_id)
#
#     logger.debug(f"After creation of p2p network workers: {config_manager.workers}")
#
#     # The config manager should have all the nodes are registered
#     assert all([config_manager.graph.has_node_by_id(x) for x in nodes_ids])
#
#     # Extract all the nodes
#     nodes_ids = []
#     for worker_id in config_manager.workers:
#         for node_id in config_manager.workers[worker_id].nodes:
#             assert config_manager.workers[worker_id].nodes[node_id].connected
#             nodes_ids.append(node_id)
#
#     # The config manager should have all the nodes registered as CONNECTED
#     assert all([x in config_manager.nodes_server_table for x in nodes_ids])
#
#     # Extract all the nodes
#     nodes_idss = []
#     for worker_id in config_manager.workers:
#         for node_id in config_manager.workers[worker_id].nodes:
#             assert config_manager.workers[worker_id].nodes[node_id].ready
#             nodes_idss.append(node_id)
#
#     # The config manager should have all the nodes registered as READY
#     assert all([x in config_manager.nodes_server_table for x in nodes_idss])


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

    # Convert the expected from name to id
    expected_output_by_id = {}
    for k, v in expected_output.items():
        id = config_manager.graph.get_id_by_name(k)
        expected_output_by_id[id] = v

    # Assert
    for k, v in expected_output_by_id.items():
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

    # Convert the expected from name to id
    expected_output_by_id = {}
    for k, v in expected_output.items():
        id = config_manager.graph.get_id_by_name(k)
        expected_output_by_id[id] = v

    # Assert
    for k, v in expected_output_by_id.items():
        assert (
            k in latest_data_values
            and isinstance(latest_data_values[k], cp.DataChunk)
            and latest_data_values[k].get("default")["value"] == v
        )
