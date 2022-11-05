from typing import Dict, Optional, List, Union
import socket
import logging
import pdb
import time

import dill

logger = logging.getLogger("chimerapy")

import networkx as nx

from .server import Server
from .graph import Graph
from . import enums


class Manager:
    def __init__(self, port: int = 9000, max_num_of_workers: int = 50):
        """Create ``Manager``, the controller of the cluster.

        The ``Manager`` is the director of the cluster, such as adding
        new computers, providing roles, starting and stopping data
        collection, and shutting down the system.

        Args:
            port (int): Referred port, might return a different one based\
            on availablity.
            max_num_of_workers (int): max_num_of_workers

        """
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

    def update_nodes_status(self, msg: Dict, worker_socket: socket.socket):

        # Updating nodes status
        self.workers[msg["data"]["name"]]["nodes_status"] = msg["data"]["nodes_status"]
        self.workers[msg["data"]["name"]]["response"] = True

        logger.info(f"{self}: Nodes status update to: {self.workers}")

    def complete_worker_broadcast(self, msg: Dict, worker_socket: socket.socket):

        # Tracking which workers have responded
        self.workers[msg["data"]["name"]]["served_nodes_server_data"] = True

    def register_graph(self, graph: Graph):
        """Verifying that a Graph is valid, that is a DAG.

        In ChimeraPy, cycle are not allowed, this is to avoid a deadlock.
        Registering the graph is the first step to setting up the data
        pipeline in  the cluster.

        Args:
            graph (Graph): A directed acyclic graph.

        """
        # First, check if graph is valid
        if not graph.is_valid():
            logger.error("Invalid Graph - rejected!")
            return

        # Else, let's save it
        self.graph = graph
        self.commitable_graph = False

    def map_graph(self, worker_graph_map: Dict[str, List[str]]):
        """Mapping ``Node`` from graph to ``Worker`` from cluster.

        The mapping, a dictionary, informs ChimeraPy which ``Worker`` is
        going to execute which ``Node``s.

        Args:
            worker_graph_map (Dict[str, List[str]]): The keys are the \
            ``Worker``'s name and the values should be a list of \
            the ``Node``'s names that will be executed within its corresponding\
            ``Worker`` key.

        """
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
                    "pickled": dill.dumps(self.graph.G.nodes[node_name]["object"]),
                    "in_bound": list(self.graph.G.predecessors(node_name)),
                    "out_bound": list(self.graph.G.successors(node_name)),
                },
            },
        )

    def wait_until_node_creation_complete(
        self, worker_name: str, node_name: str, timeout: Union[int, float] = 20
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
                    raise RuntimeError(
                        f"Manager waiting for {node_name} not sending INIT"
                    )
                miss_counter += 1

    def create_p2p_network(self):

        # Send the message to each worker
        for worker_name in self.worker_graph_map:

            # Send each node that needs to be constructed
            for node_name in self.worker_graph_map[worker_name]:

                fail_attempts = 0
                while True:

                    # If fail too many times, give up :(
                    if fail_attempts > 10:
                        raise RuntimeError(
                            f"Couldn't create {node_name} after multiple tries"
                        )

                    # Send request to create node
                    self.request_node_creation(worker_name, node_name)

                    # Wait until the node has been created and connected
                    # to the corresponding worker
                    try:
                        self.wait_until_node_creation_complete(worker_name, node_name)
                        break
                    except RuntimeError:
                        fail_attempts += 1
                        logger.warning(
                            f"Node creation failed: {worker_name}:{node_name}, attempt {fail_attempts}"
                        )

                logger.debug(f"Creation of Node {node_name} successful")

    def request_node_server_data(self):

        # Reset responses
        self.mark_response_as_false_for_workers()

        # Send the message to each worker and wait
        for worker_name in self.worker_graph_map:

            fail_attempts = 0
            while True:

                # If too many attempts, give up :(
                if fail_attempts > 10:
                    raise RuntimeError(
                        "Worker: {worker_name} not responsive after multiple attempts"
                    )

                # Send the request to each worker
                self.server.send(
                    self.workers[worker_name]["socket"],
                    {"signal": enums.MANAGER_REQUEST_NODE_SERVER_DATA, "data": {}},
                )

                # Wait until response
                try:
                    self.wait_until_worker_respond(
                        worker_name, attribute="reported_nodes_server_data"
                    )
                    break
                except:
                    fail_attempts += 1
                    logger.warning(
                        f"Failed to receive Worker's node server request: attempt {fail_attempts}"
                    )

        logger.debug("Node data server gathering successful")

    def request_connection_creation(self):

        # Reset responses
        self.mark_response_as_false_for_workers()

        # Send the message to each worker and wait
        for worker_name in self.worker_graph_map:

            fail_attempts = 0
            while True:

                # If too many attempts, give up :(
                if fail_attempts > 10:
                    raise RuntimeError(
                        "Worker: {worker_name} not responsive after multiple attempts"
                    )

                # Send the request to each worker
                self.server.send(
                    self.workers[worker_name]["socket"],
                    {
                        "signal": enums.MANAGER_BROADCAST_NODE_SERVER_DATA,
                        "data": self.nodes_server_table,
                    },
                )

                # Wait until response
                try:
                    self.wait_until_worker_respond(
                        worker_name, attribute="served_nodes_server_data"
                    )
                    break
                except:
                    fail_attempts += 1
                    logger.warning(
                        f"Failed to receive Worker's node server request: attempt {fail_attempts}"
                    )

        logger.debug("Graph connections successful")

    def setup_p2p_connections(self):

        # After all nodes have been created, get the entire graphs
        # host and port information
        self.nodes_server_table = {}

        # Broadcast request for node server data
        self.request_node_server_data()

        # Distribute the entire graph's information to all the Workers
        self.request_connection_creation()

    def commit_graph(self):
        """Committing ``Graph`` to the cluster.

        Committing refers to how the graph itself (with its nodes and edges)
        and the mapping is distributed to the cluster. The whole routine
        is two steps: peer creation and peer-to-peer connection setup.

        In peer creation, the ``Manager`` messages each ``Worker`` with
        the ``Nodes`` they need to execute. The ``Workers`` configure
        the ``Nodes``, by giving them network information. The ``Nodes``
        are then started and report back to the ``Workers``.

        With the successful creation of the ``Nodes``, the ``Manager``
        request the ``Nodes`` servers' ip address and port numbers to
        create an address table for all the ``Nodes``. Then this table
        is used to inform each ``Node`` where their in-bound and out-bound
        ``Nodes`` are located; thereby establishing the edges between
        ``Nodes``.

        """
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

    def wait_until_worker_respond(
        self,
        worker_name: str,
        attribute: str = "response",
        timeout: Union[int, float] = 10,
        delay: float = 0.1,
    ):

        miss_counter = 0
        while True:

            # IF the worker responded, then break
            if self.workers[worker_name][attribute]:
                break

            time.sleep(delay)

            if delay * miss_counter > timeout:
                logger.error(f"{self}: stuck")
                raise RuntimeError(f"{self}: Worker {worker_name} did not respond!")

            miss_counter += 1

    def wait_until_all_workers_responded(
        self, attribute: str, timeout: Union[float, int] = 10, delay: float = 0.1
    ):

        # Waiting until every worker has responded and provided their
        # data
        for worker_name in self.workers:
            self.wait_until_worker_respond(worker_name, attribute, timeout, delay)

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
        """Cluster step execution for offline operation.

        The ``step`` function is for careful but slow operation of the
        cluster. For online execution, ``start`` and ``stop`` are the
        methods to be used.

        """
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
        """Start the executiong of the cluster.

        Before starting, make sure that you have perform the following
        steps:

        - Create ``Nodes``
        - Create ``DAG`` with ``Nodes`` and their edges
        - Connect ``Workers`` (must be before committing ``Graph``)
        - Register, map, and commit ``Graph``

        """
        self.server.broadcast({"signal": enums.MANAGER_START_NODES, "data": {}})

    def stop(self):
        """Stop the executiong of the cluster.

        Do not forget that you still need to execute ``shutdown`` to
        properly shutdown processes, threads, and queues.

        """
        self.server.broadcast({"signal": enums.MANAGER_STOP_NODES, "data": {}})

    def shutdown(self):
        """Proper shutting down ChimeraPy cluster.

        Through this method, the ``Manager`` broadcast to all ``Workers``
        to shutdown, in which they will stop their processes and threads safely.

        """
        # First, shutdown server
        self.server.shutdown()
