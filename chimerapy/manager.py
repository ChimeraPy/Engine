from typing import Dict, Optional, List, Union
import socket
import logging
import pdb
import time

import jsonpickle
import dill

logger = logging.getLogger("chimerapy")

import networkx as nx

from .server import Server
from .graph import Graph
from .utils import log
from . import enums


class Manager:
    def __init__(self, port: int = 9000, max_num_of_workers: int = 50):

        # Saving input parameters
        # self.host = socket.gethostbyname(socket.gethostname())
        self.port = port
        self.max_num_of_workers = max_num_of_workers

        # Instance variables
        self.workers: Dict = {}
        self.graph: Graph = Graph()
        self.worker_graph_map: Dict = {}
        self.commitable_graph: bool = False
        self.nodes_server_table: Dict = {}

        # Specifying the handlers for worker2manager communication
        self.handlers = {
            enums.WORKER_REGISTER: self.register_worker,
            enums.WORKER_DEREGISTER: self.deregister_worker,
            enums.WORKER_REPORT_NODE_SERVER_DATA: self.node_server_data,
            enums.WORKER_REPORT_NODES_STATUS: self.update_nodes_status,
            enums.WORKER_COMPLETE_BROADCAST: self.complete_worker_broadcast,
            enums.WORKER_REPORT_GATHER: self.get_gather,
        }

        # Create server
        self.server = Server(
            port=self.port,
            name="Manager",
            max_num_of_clients=self.max_num_of_workers,
            sender_msg_type=enums.MANAGER_MESSAGE,
            accepted_msg_type=enums.WORKER_MESSAGE,
            handlers=self.handlers,
        )
        self.server.start()
        logger.info(f"Server started at Port {self.server.port}")

        # Updating the manager's port to the found available port
        self.host, self.port = self.server.host, self.server.port

    def register_worker(self, msg: Dict, worker_socket: socket.socket):
        self.workers[msg["data"]["name"]] = {
            "addr": msg["data"]["addr"],
            "socket": worker_socket,
            "ack": True,
            "response": False,
            "reported_nodes_server_data": False,
            "served_nodes_server_data": False,
            "nodes_status": {},
            "gather": {},
        }

    def deregister_worker(self, msg: Dict, worker_socket: socket.socket):
        worker_socket.close()
        del self.workers[msg["data"]["name"]]

    def node_server_data(self, msg: Dict, worker_socket: socket.socket):

        # And then updating the node server data
        self.nodes_server_table.update(msg["data"]["nodes"])

        # Tracking which workers have responded
        self.workers[msg["data"]["name"]]["reported_nodes_server_data"] = True
        self.workers[msg["data"]["name"]]["response"] = True

    def update_nodes_status(self, msg: Dict, worker_socket: socket.socket):

        # Updating nodes status
        self.workers[msg["data"]["name"]]["nodes_status"] = msg["data"]["nodes_status"]
        self.workers[msg["data"]["name"]]["response"] = True

        logger.info(f"{self}: Nodes status update to: {self.workers}")

    def complete_worker_broadcast(self, msg: Dict, worker_socket: socket.socket):

        # Tracking which workers have responded
        self.workers[msg["data"]["name"]]["served_nodes_server_data"] = True
        self.workers[msg["data"]["name"]]["response"] = True

    def register_graph(self, graph: Graph):

        # First, check if graph is valid
        if not graph.is_valid():
            logger.error("Invalid Graph - rejected!")
            return

        # Else, let's save it
        self.graph = graph
        self.commitable_graph = False

    def map_graph(self, worker_graph_map: Dict[str, List[str]]):

        # Tracking valid inputs
        checks = []

        # First, check if the mapping is valid!
        for worker_name in worker_graph_map:

            # First check if the worker is registered!
            if worker_name not in self.workers:
                logger.error(f"{worker_name} is not register to Manager.")
                checks.append(False)
                break
            else:
                checks.append(True)

            # Then check if the node exists in the graph
            for node_name in worker_graph_map[worker_name]:
                if not self.graph.has_node_by_name(node_name):
                    logger.error(f"{node_name} is not in Manager's graph.")
                    checks.append(False)
                    break
                else:
                    checks.append(True)

        # If everything is okay, we are approved to commit the graph
        if all(checks):
            self.commitable_graph = True
        else:
            self.commitable_graph = False

        # Save the worker graph
        self.worker_graph_map = worker_graph_map

    def request_node_creation(self, worker_name: str, node_name: str):

        if isinstance(self.worker_graph_map, nx.Graph):
            logger.warning(f"Cannot create Node {node_name} with Worker {worker_name}")
            return

        # Send for the creation of the node
        self.server.send(
            self.workers[worker_name]["socket"],
            {
                "signal": enums.MANAGER_CREATE_NODE,
                "data": {
                    "worker_name": worker_name,
                    "node_name": node_name,
                    "node_object": dill.dumps(self.graph.G.nodes[node_name]["object"]),
                    "in_bound": list(self.graph.G.predecessors(node_name)),
                    "out_bound": list(self.graph.G.successors(node_name)),
                },
            },
        )

    def wait_until_node_creation_complete(
        self, worker_name: str, node_name: str, timeout: Union[int, float] = 10
    ):

        delay = 0.1
        miss_counter = 0
        while True:
            time.sleep(delay)

            if (
                node_name in self.workers[worker_name]["nodes_status"]
                and self.workers[worker_name]["nodes_status"][node_name]["INIT"]
            ):
                break
            else:
                if delay * miss_counter > timeout:
                    ...
                    # raise RuntimeError(
                    #     f"Manager waiting for {node_name} not sending INIT"
                    # )
                miss_counter += 1

        # Perform health check on the worker
        # self.server.send(
        #     self.workers[worker_name]["socket"],
        #     {"signal": enums.MANAGER_HEALTH_CHECK, "data": {}},
        #     ack=True,
        # )

    def create_p2p_network(self):

        # Send the message to each worker
        for worker_name in self.worker_graph_map:

            # Send each node that needs to be constructed
            for node_name in self.worker_graph_map[worker_name]:

                # Send request to create node
                self.request_node_creation(worker_name, node_name)

                # Wait until the node has been created and connected
                # to the corresponding worker
                self.wait_until_node_creation_complete(worker_name, node_name)

                # Mark the response as false, since it is not used
                self.mark_response_as_false_for_workers()

                logger.debug(f"Creation of Node {node_name} successful")

    def setup_p2p_connections(self):

        # After all nodes have been created, get the entire graphs
        # host and port information
        self.nodes_server_table = {}

        # Broadcast request for node server data
        self.mark_response_as_false_for_workers()
        self.server.broadcast(
            {
                "signal": enums.MANAGER_REQUEST_NODE_SERVER_DATA,
                "data": {},
            }
        )

        # Wail until all workers have responded with their node server data
        self.wait_until_all_workers_responded(
            attribute="reported_nodes_server_data", timeout=5
        )

        # Distribute the entire graph's information to all the Workers
        self.mark_response_as_false_for_workers()
        self.server.broadcast(
            {
                "signal": enums.MANAGER_BROADCAST_NODE_SERVER_DATA,
                "data": self.nodes_server_table,
            }
        )

        # Wail until all workers have claimed that their nodes are ready
        self.wait_until_all_workers_responded(
            attribute="served_nodes_server_data", timeout=5
        )

    def commit_graph(self):

        # First, check that the graph has been cleared
        assert self.commitable_graph, logger.error(
            "Tried to commit graph that hasn't been confirmed or ready"
        )

        # First, create the network
        self.create_p2p_network()

        # Then setup the p2p connections between nodes
        self.setup_p2p_connections()

    def mark_response_as_false_for_workers(self):

        for worker_name in self.workers:
            self.workers[worker_name]["response"] = False

    def wait_until_all_workers_responded(
        self, attribute: str, timeout: Optional[float] = None, delay: float = 0.1
    ):

        # Waiting until every worker has responded and provided their
        # data
        for worker_name in self.workers:

            miss_counter = 0
            while True:

                # IF the worker responded, then break
                if self.workers[worker_name]["response"]:
                    break

                time.sleep(delay)

                if (
                    isinstance(timeout, (float, int))
                    and miss_counter * delay >= timeout
                ):
                    logger.error(f"{self}: stuck")
                    raise RuntimeError(f"{self}: Worker {worker_name} did not respond!")

                miss_counter += 1

    def check_all_nodes_ready(self):

        logger.debug(f"Checking node ready: {self.workers}")

        # Tracking all results and avoiding a default True value
        checks = []

        # Iterate through all the workers
        for worker_name in self.workers:

            # Iterate through their nodes
            for node_name in self.workers[worker_name]["nodes_status"]:
                checks.append(
                    self.workers[worker_name]["nodes_status"][node_name]["READY"]
                )

        # Only if all checks are True can we say the p2p network is ready
        logger.debug(f"checks: {checks}")
        if all(checks):
            return True
        else:
            return False

    def wait_until_all_nodes_ready(
        self, timeout: Optional[Union[float, int]] = None, delay: float = 0.1
    ):

        miss_counter = 0
        while not self.check_all_nodes_ready():
            logger.debug("Waiting for nodes to be ready")
            time.sleep(delay)

            if isinstance(timeout, (float, int)) and miss_counter * delay >= timeout:
                logger.error("Node ready timeout")
                raise RuntimeError("Node ready timeout")

            miss_counter += 1

    def step(self):

        # First, the step should only be possible after a graph has
        # been commited
        assert (
            self.check_all_nodes_ready()
        ), "Manager cannot take step if Nodes not ready"

        # Send message for workers to inform all nodes to take a single
        # step
        self.server.broadcast({"signal": enums.MANAGER_REQUEST_STEP, "data": {}})

    def get_gather(self, msg: Dict, worker_socket: socket.socket):

        self.workers[msg["data"]["name"]]["gather"] = msg["data"]["node_data"]
        self.workers[msg["data"]["name"]]["response"] = True

    def gather(self) -> Dict:

        # First, the step should only be possible after a graph has
        # been commited
        assert self.check_all_nodes_ready(), "Manager cannot gather if Nodes not ready"

        # Mark workers as not responded yet
        self.mark_response_as_false_for_workers()

        # Request gathering the latest value of each node
        self.server.broadcast({"signal": enums.MANAGER_REQUEST_GATHER, "data": {}})

        # Wail until all workers have responded with their node server data
        self.wait_until_all_workers_responded(attribute="gather", timeout=5)

        # Extract the gather data
        gather_data = {}
        for worker_data in self.workers.values():
            gather_data.update(worker_data["gather"])

        return gather_data

    def start(self):
        self.server.broadcast({"signal": enums.MANAGER_START_NODES, "data": {}})

    def stop(self):
        self.server.broadcast({"signal": enums.MANAGER_STOP_NODES, "data": {}})

    def shutdown(self):

        # First, shutdown server
        self.server.shutdown()
