# Built-in Imports
import logging
import time
import dill

# Third-party Imports
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")

# class PrepInfiniteNode(cp.Node):

#     def prep(self):
#         while True:
#             time.sleep(1)


# def test_node_start_hanging(worker):

#     faulty_node = PrepInfiniteNode(name='test')

#     # Simple single node without connection
#     msg = {
#         "data": {
#             "node_name": faulty_node.name,
#             "pickled": dill.dumps(faulty_node),
#             "in_bound": [],
#             "out_bound": [],
#             "follow": None,
#         }
#     }

#     logger.debug("Create nodes")
#     worker.create_node(msg)


def test_faulty_commit_graph_mapping(gen_node, con_node):

    graph = cp.Graph()
    graph.add_nodes_from([gen_node, con_node])
