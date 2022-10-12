import time
import logging

logger = logging.getLogger("chimerapy")

import pytest
import jsonpickle

import chimerapy as cp


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
