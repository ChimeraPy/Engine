# Built-in Imports
import time
import logging
import pdb

# Third-party Imports
import dill

# Internal Imports
import chimerapy as cp

logger = logging.getLogger("chimerapy")
from .data_nodes import VideoNode, AudioNode, ImageNode, TabularNode

# References:
# https://www.thepythoncode.com/article/send-receive-files-using-sockets-python

NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    "an": AudioNode,
}


def test_worker_data_archiving(worker):

    # Just for debugging
    # worker.delete_temp = False

    nodes = []
    for node_name, node_class in NAME_CLASS_MAP.items():
        nodes.append(node_class(name=node_name))

    # Simple single node without connection
    for node in nodes:
        msg = {
            "data": {
                "node_name": node.name,
                "pickled": dill.dumps(node),
                "in_bound": [],
                "out_bound": [],
                "follow": None,
            }
        }
        worker.create_node(msg)

    logger.debug("Waiting!")
    time.sleep(2)

    logger.debug("Start nodes!")
    worker.start_nodes({})

    logger.debug("Let nodes run for some time")
    time.sleep(1)

    for node_name in NAME_CLASS_MAP:
        assert (worker.tempfolder / node_name).exists()


def test_manager_worker_data_transfer(manager, worker):

    # Define graph
    graph = cp.Graph()
    for node_name, node_class in NAME_CLASS_MAP.items():
        graph.add_node(node_class(name=node_name))

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            worker.name: list(NAME_CLASS_MAP.keys()),
        }
    )

    # Commiting the graph by sending it to the workers
    manager.commit_graph()
    manager.wait_until_all_nodes_ready(timeout=10)

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(2)
    manager.stop()

    # Transfer the files to the Manager's logs
    manager.collect()
