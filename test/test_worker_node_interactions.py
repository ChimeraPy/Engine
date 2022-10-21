import time
import logging

logger = logging.getLogger("chimerapy")

import pytest
import jsonpickle

import chimerapy as cp

from .conftest import GenNode, ConsumeNode


@pytest.mark.repeat(3)
def test_worker_create_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "node_object": jsonpickle.dumps(gen_node),
            "in_bound": [],
            "out_bound": [],
        }
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Finishied creating nodes")
    assert gen_node.name in worker.nodes


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
                "node_object": jsonpickle.dumps(new_node),
                "in_bound": [],
                "out_bound": [],
            }
        }

        msg2 = {
            "data": {
                "node_name": new_node2.name,
                "node_object": jsonpickle.dumps(new_node2),
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


def test_step_single_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "node_object": jsonpickle.dumps(gen_node),
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


def test_two_nodes_connect(worker, gen_node, con_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "node_object": jsonpickle.dumps(gen_node),
            "in_bound": [],
            "out_bound": [con_node.name],
        }
    }

    # Simple single node without connection
    msg2 = {
        "data": {
            "node_name": con_node.name,
            "node_object": jsonpickle.dumps(con_node),
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


def test_starting_node(worker, gen_node):

    # Simple single node without connection
    msg = {
        "data": {
            "node_name": gen_node.name,
            "node_object": jsonpickle.dumps(gen_node),
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


def test_manager_directing_worker_to_create_node(manager, worker):

    # Create original containers
    simple_graph = cp.Graph()
    new_node = GenNode(name=f"Gen1")
    simple_graph.add_nodes_from([new_node])
    mapping = {worker.name: [new_node.name]}

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(mapping)

    # Request node creation
    manager.request_node_creation(worker_name=worker.name, node_name="Gen1")
    manager.wait_until_node_creation_complete(worker_name=worker.name, node_name="Gen1")


# @pytest.mark.repeat(100)
def test_stress_manager_directing_worker_to_create_node(manager, worker):

    # Create original containers
    simple_graph = cp.Graph()
    to_be_created_nodes = []
    mapping = {worker.name: []}

    for i in range(5):

        # Create node and save name for later comparison
        new_node = GenNode(name=f"Gen{i}")
        new_node2 = ConsumeNode(name=f"Con{i}")
        to_be_created_nodes.append(new_node.name)
        to_be_created_nodes.append(new_node2.name)

        # Simple single node without connection
        simple_graph.add_nodes_from([new_node, new_node2])

        # Create mapping
        mapping[worker.name].append(new_node.name)
        mapping[worker.name].append(new_node2.name)

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(simple_graph)

    # Specify what nodes to what worker
    manager.map_graph(mapping)

    # Request node creation
    for node_name in to_be_created_nodes:
        manager.request_node_creation(worker_name=worker.name, node_name=node_name)
        manager.wait_until_node_creation_complete(
            worker_name=worker.name, node_name=node_name
        )
