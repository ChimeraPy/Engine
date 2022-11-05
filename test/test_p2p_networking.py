import time
import sys
import pdb
import logging
import platform

import pytest
from pytest_lazyfixture import lazy_fixture

from chimerapy import Worker, Graph, Node
from .conftest import linux_run_only, linux_expected_only
from .mock import DockeredWorker

logger = logging.getLogger("chimerapy")


@pytest.fixture
def graph(gen_node, con_node):

    # Define graph
    _graph = Graph()
    _graph.add_nodes_from([gen_node, con_node])
    _graph.add_edge(src=gen_node, dst=con_node)

    return _graph


@pytest.fixture
def single_node_no_connections_manager(manager, worker, gen_node):

    # Define graph
    simple_graph = Graph()
    simple_graph.add_nodes_from([gen_node])

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            "local": ["Gen1"],
        }
    )

    return manager


@pytest.fixture
def multiple_nodes_one_worker_manager(manager, worker, graph):

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            "local": ["Gen1", "Con1"],
        }
    )

    return manager


@pytest.fixture
def multiple_nodes_multiple_workers_manager(manager, graph):

    worker1 = Worker(name="local")
    worker2 = Worker(name="local2")

    worker1.connect(host=manager.host, port=manager.port)
    worker2.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(graph)

    # Specify what nodes to what worker
    manager.map_graph({"local": ["Gen1"], "local2": ["Con1"]})

    yield manager

    worker1.shutdown()
    worker2.shutdown()


@pytest.fixture
def slow_single_node_single_worker_manager(manager, worker, slow_node):

    # Define graph
    simple_graph = Graph()
    simple_graph.add_nodes_from([slow_node])

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            "local": ["Slo1"],
        }
    )

    return manager


@pytest.fixture
def dockered_single_node_no_connections_manager(dockered_worker, manager, gen_node):

    # Define graph
    simple_graph = Graph()
    simple_graph.add_nodes_from([gen_node])

    # Connect to the manager
    dockered_worker.connect(host=manager.host, port=manager.port)
    # dockered_worker.connect(host='10.0.0.153', port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            dockered_worker.name: ["Gen1"],
        }
    )

    return manager


@pytest.fixture
def dockered_multiple_nodes_one_worker_manager(
    dockered_worker, manager, gen_node, con_node
):

    # Define graph
    simple_graph = Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(gen_node, con_node)

    # Connect to the manager
    dockered_worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            dockered_worker.name: ["Gen1", "Con1"],
        }
    )

    return manager


@pytest.fixture
def dockered_multiple_nodes_multiple_workers_manager(docker_client, manager, graph):

    worker1 = DockeredWorker(docker_client, name="local")
    worker2 = DockeredWorker(docker_client, name="local2")

    worker1.connect(host=manager.host, port=manager.port)
    worker2.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(graph)

    # Specify what nodes to what worker
    manager.map_graph({"local": ["Gen1"], "local2": ["Con1"]})

    yield manager

    worker1.shutdown()
    worker2.shutdown()


@pytest.mark.repeat(10)
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
def test_p2p_network_creation(config_manager, expected_worker_to_nodes):

    # Commiting the graph by sending it to the workers
    config_manager.create_p2p_network()

    logger.info(config_manager.workers)

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


@pytest.mark.repeat(10)
@pytest.mark.parametrize(
    "config_manager",
    [
        (lazy_fixture("single_node_no_connections_manager")),
        (lazy_fixture("multiple_nodes_one_worker_manager")),
        (lazy_fixture("multiple_nodes_multiple_workers_manager")),
        pytest.param(
            lazy_fixture("dockered_single_node_no_connections_manager"),
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
            marks=linux_run_only,
        ),
    ],
)
def test_p2p_network_connections(config_manager):

    # Commiting the graph by sending it to the workers
    config_manager.create_p2p_network()
    config_manager.setup_p2p_connections()

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


@pytest.mark.parametrize(
    "config_manager",
    [
        (lazy_fixture("single_node_no_connections_manager")),
        (lazy_fixture("multiple_nodes_one_worker_manager")),
        (lazy_fixture("multiple_nodes_multiple_workers_manager")),
        (lazy_fixture("slow_single_node_single_worker_manager")),
        pytest.param(
            lazy_fixture("dockered_single_node_no_connections_manager"),
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
            marks=linux_run_only,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
            marks=linux_run_only,
        ),
    ],
)
def test_detecting_when_all_nodes_are_ready(config_manager):

    # Commiting the graph by sending it to the workers
    config_manager.commit_graph()
    config_manager.wait_until_all_nodes_ready(timeout=10)

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

    # Commiting the graph by sending it to the workers
    config_manager.commit_graph()
    config_manager.wait_until_all_nodes_ready(timeout=10)

    # Take a single step and see if the system crashes and burns
    config_manager.step()
    time.sleep(2)

    # Then request gather and confirm that the data is valid
    latest_data_values = config_manager.gather()

    # Assert
    for k, v in expected_output.items():
        assert k in latest_data_values and latest_data_values[k] == v


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

    # Commiting the graph by sending it to the workers
    config_manager.commit_graph()
    config_manager.wait_until_all_nodes_ready(timeout=10)

    # Take a single step and see if the system crashes and burns
    config_manager.start()
    time.sleep(2)
    config_manager.stop()

    # Then request gather and confirm that the data is valid
    latest_data_values = config_manager.gather()

    # Assert
    for k, v in expected_output.items():
        assert k in latest_data_values and latest_data_values[k] == v
