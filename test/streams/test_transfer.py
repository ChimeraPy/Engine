# Built-in Imports
import time
import logging

# Third-party Imports
import dill

# Internal Imports
import chimerapy as cp

logger = logging.getLogger("chimerapy")
from .data_nodes import VideoNode, AudioNode, ImageNode, TabularNode

# References:
# https://www.thepythoncode.com/article/send-receive-files-using-sockets-python


def test_worker_data_archiving(worker):

    name_class_map = {
        "vn": VideoNode,
        "img_n": ImageNode,
        "tn": TabularNode,
        "an": AudioNode,
    }
    nodes = []
    for node_name, node_class in name_class_map.items():
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

    for node_name in name_class_map:
        assert (worker.logdir / node_name).exists()
