from typing import Dict, Optional, List, Union, Any
import socket
import pdb
import pathlib
import os
import time
import datetime
import json
import random
import tempfile
import zipfile
from concurrent.futures import wait

# Third-party Imports
import dill
import aiohttp
from aiohttp import web
import networkx as nx

from chimerapy import config
from .networking import Server, Client
from .networking.enums import MANAGER_MESSAGE, WORKER_MESSAGE
from .graph import Graph
from .exceptions import CommitGraphError
from . import _logger
from .utils import waiting_for

logger = _logger.getLogger("chimerapy")


class Manager:
    def __init__(
        self,
        logdir: pathlib.Path,
        port: int = 8080,
        max_num_of_workers: int = 50,
    ):
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
        self.name = "Manager"
        self.host = "localhost"
        self.port = port
        self.max_num_of_workers = max_num_of_workers
        self.has_shutdown = False

        # Create log directory to store data
        timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        rand_num = random.randint(1000, 9999)
        self.logdir = logdir / f"chimerapy-{timestamp}-{rand_num}"

        # Also create a tempfolder to store any miscellaneous files and folders
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        # Create a logging directory
        os.makedirs(self.logdir, exist_ok=True)

        # Instance variables
        self.workers: Dict[str, Dict[str, Any]] = {}
        self.graph: Graph = Graph()
        self.worker_graph_map: Dict = {}
        self.commitable_graph: bool = False
        self.nodes_server_table: Dict = {}
        self.start_time: Optional[datetime.datetime] = None
        self.stop_time: Optional[datetime.datetime] = None
        self.duration: int = 0

        # Create server
        self.server = Server(
            port=self.port,
            name="Manager",
            ws_handlers={
                WORKER_MESSAGE.REGISTER: self.register_worker,
                WORKER_MESSAGE.DEREGISTER: self.deregister_worker,
                WORKER_MESSAGE.REPORT_NODE_SERVER_DATA: self.node_server_data,
                WORKER_MESSAGE.REPORT_NODES_STATUS: self.update_nodes_status,
                WORKER_MESSAGE.COMPLETE_BROADCAST: self.complete_worker_broadcast,
                WORKER_MESSAGE.REPORT_GATHER: self.get_gather,
                WORKER_MESSAGE.PACKAGE_LOADED: self.complete_worker_load_package,
                WORKER_MESSAGE.TRANSFER_COMPLETE: self.complete_worker_transfer,
            },
        )
        self.server.serve()
        logger.info(f"Manager started at {self.server.host}:{self.server.port}")

        # Updating the manager's port to the found available port
        self.host, self.port = self.server.host, self.server.port

    def __repr__(self):
        return f"<Manager @{self.host}:{self.port}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Message Reactivity API
    ####################################################################

    async def register_worker(self, msg: Dict, ws: web.WebSocketResponse):
        self.workers[msg["data"]["name"]] = {
            "addr": msg["data"]["addr"],
            "http_port": msg["data"]["http_port"],
            "http_ip": msg["data"]["http_ip"],
            "ws": ws,
            "ack": True,
            "response": False,
            "reported_nodes_server_data": False,
            "served_nodes_server_data": False,
            "nodes_status": {},
            "gather": {},
            "collection_complete": False,
            "package_loaded": False,
        }
        logger.info(
            f"Manager registered <Worker name={msg['data']['name']}> from {msg['data']['addr']}"
        )

    async def deregister_worker(self, msg: Dict, ws: web.WebSocketResponse):
        await ws.close()
        logger.info(
            f"Manager deregistered <Worker name={msg['data']['name']}> from {msg['data']['addr']}"
        )
        del self.workers[msg["data"]["name"]]

    async def node_server_data(self, msg: Dict, ws: web.WebSocketResponse):

        # And then updating the node server data
        self.nodes_server_table.update(msg["data"]["nodes"])

        # Tracking which workers have responded
        self.workers[msg["data"]["name"]]["reported_nodes_server_data"] = True

    async def update_nodes_status(self, msg: Dict, ws: web.WebSocketResponse):

        # Updating nodes status
        self.workers[msg["data"]["name"]]["nodes_status"] = msg["data"]["nodes_status"]
        self.workers[msg["data"]["name"]]["response"] = True

        logger.debug(f"{self}: Nodes status update to: {self.workers}")

    async def complete_worker_broadcast(self, msg: Dict, ws: web.WebSocketResponse):

        # Tracking which workers have responded
        self.workers[msg["data"]["name"]]["served_nodes_server_data"] = True

    async def get_gather(self, msg: Dict, ws: web.WebSocketResponse):

        self.workers[msg["data"]["name"]]["gather"] = msg["data"]["node_data"]
        self.workers[msg["data"]["name"]]["response"] = True

    async def complete_worker_load_package(self, msg: Dict, ws: web.WebSocketResponse):

        self.workers[msg["data"]["name"]]["package_loaded"] = True
        self.workers[msg["data"]["name"]]["response"] = True

    async def complete_worker_transfer(self, msg: Dict, ws: web.WebSocketResponse):

        self.workers[msg["data"]["name"]]["collection_complete"] = True
        self.workers[msg["data"]["name"]]["response"] = True

    ####################################################################
    ## Helper Methods
    ####################################################################

    def save_meta(self):

        # Get the times, handle Optional
        if self.start_time:
            start_time = self.start_time.strftime("%Y_%m_%d_%H_%M_%S")
        else:
            start_time = None

        if self.stop_time:
            stop_time = self.stop_time.strftime("%Y_%m_%d_%H_%M_%S")
        else:
            stop_time = None

        # Generate meta record
        meta = {
            "workers": list(self.workers.keys()),
            "nodes": list(self.graph.G.nodes()),
            "worker_graph_map": self.worker_graph_map,
            "nodes_server_table": self.nodes_server_table,
            "start_time": start_time,
            "stop_time": stop_time,
        }

        with open(self.logdir / "meta.json", "w") as f:
            json.dump(meta, f, indent=2)

    def mark_response_as_false_for_workers(self):

        for worker_name in self.workers:
            self.workers[worker_name]["response"] = False

    def request_node_creation(self, worker_name: str, node_name: str) -> bool:
        """Request creating a Node from the Graph.

        Args:
            worker_name (str): The targetted Worker
            node_name (str): The name of the node to create, that is in\
            in the graph

        Returns:
            bool: Success in creating the Node

        """
        success = False
        for i in range(config.get("manager.allowed-failures")):

            # Send request to create node
            if isinstance(self.worker_graph_map, nx.Graph):
                logger.warning(
                    f"Cannot create Node {node_name} with Worker {worker_name}"
                )
                return False

            # Send for the creation of the node
            self.server.send(
                client_name=worker_name,
                signal=MANAGER_MESSAGE.CREATE_NODE,
                data={
                    "worker_name": worker_name,
                    "node_name": node_name,
                    "pickled": dill.dumps(
                        self.graph.G.nodes[node_name]["object"], recurse=True
                    ),
                    "in_bound": list(self.graph.G.predecessors(node_name)),
                    "out_bound": list(self.graph.G.successors(node_name)),
                    "follow": self.graph.G.nodes[node_name]["follow"],
                },
            )

            # Wait until the node has been created and connected
            # to the corresponding worker
            success = waiting_for(
                lambda: (node_name in self.workers[worker_name]["nodes_status"])
                and (
                    self.workers[worker_name]["nodes_status"][node_name]["INIT"] == True
                ),
                timeout=config.get("manager.timeout.node-creation"),
                msg=f"{self}: Node creation {worker_name}:{node_name}, attempt {i}",
            )

            if success:
                break

        return success

    def request_node_server_data(self) -> bool:
        """Request Workers to provide information about Node's PUBs

        Returns:
            bool: Success of obtaining the node server data

        """
        # Reset responses
        self.mark_response_as_false_for_workers()

        # Send the message to each worker and wait
        for worker_name in self.worker_graph_map:

            success = False
            for i in range(config.get("manager.allowed-failures")):

                # Send the request to each worker
                self.server.send(
                    client_name=worker_name,
                    signal=MANAGER_MESSAGE.REQUEST_NODE_SERVER_DATA,
                    data={},
                )

                # Wait until response
                success = waiting_for(
                    condition=lambda: self.workers[worker_name][
                        "reported_nodes_server_data"
                    ],
                    timeout=config.get("manager.timeout.info-request"),
                    msg=f"{self}: Requesting Worker's node server request: attempt {i}",
                )

                if success:
                    break

            if not success:
                return False

        return True

    def request_connection_creation(self) -> bool:
        """Request establishing the connections between Nodes

        This routine the Manager sends the Node's server data and request \
        for Workers to organize their own nodes.

        Returns:
            bool: Returns if connection creation was successful
        """

        # Reset responses
        self.mark_response_as_false_for_workers()

        # Send the message to each worker and wait
        for worker_name in self.worker_graph_map:

            success = False
            for i in range(config.get("manager.allowed-failures")):

                # Send the request to each worker
                self.server.send(
                    client_name=worker_name,
                    signal=MANAGER_MESSAGE.BROADCAST_NODE_SERVER_DATA,
                    data=self.nodes_server_table,
                )

                success = waiting_for(
                    condition=lambda: self.workers[worker_name][
                        "served_nodes_server_data"
                    ],
                    timeout=config.get("manager.timeout.info-request"),
                    msg=f"{self}: receiving Worker's node server request, attempt {i}",
                )

                # Wait until response
                if success:
                    break

            if not success:
                return False

        return True

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

    def check_all_nodes_saved(self):

        logger.debug(f"Checking node ready: {self.workers}")

        # Tracking all results and avoiding a default True value
        checks = []

        # Iterate through all the workers
        for worker_name in self.workers:

            # Iterate through their nodes
            for node_name in self.workers[worker_name]["nodes_status"]:
                checks.append(
                    self.workers[worker_name]["nodes_status"][node_name]["FINISHED"]
                )

        # Only if all checks are True can we say the p2p network is ready
        logger.debug(f"checks: {checks}")
        if all(checks):
            return True
        else:
            return False

    def wait_until_all_nodes_ready(
        self, timeout: Optional[Union[float, int]] = None
    ) -> bool:
        """Wait until all Nodes have initialized, connected, and ready to go.

        Args:
            timeout (Optional[Union[float, int]]): Optional timeout

        Returns:
            bool: Success if all ``Nodes`` are ready.
        """

        if not timeout:
            timeout = config.get("manager.timeout.info-request")

        success = False
        for i in range(config.get("manager.allowed-failures")):

            success = waiting_for(
                condition=self.check_all_nodes_ready,
                msg=f"{self}: Waited for Nodes to be ready",
                timeout=config.get("manager.timeout.info-request"),
            )

            if success:
                break

        return success

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
            raise CommitGraphError("Invalid Graph, not DAG - rejected!")

        # Else, let's save it
        self.graph = graph

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

        if not all(checks):
            raise CommitGraphError("Mapping is invalid")

        # Save the worker graph
        self.worker_graph_map = worker_graph_map

    ####################################################################
    ## Cluster Setup, Control, and Monitor API
    ####################################################################

    def create_p2p_network(self) -> bool:
        """Create the P2P Nodes in the Network

        This routine only creates the Node via the Workers but doesn't \
        establish the connections.

        Returns:
            bool: Success in creating the P2P Nodes

        """
        # Send the message to each worker
        for worker_name in self.worker_graph_map:

            # Send each node that needs to be constructed
            for node_name in self.worker_graph_map[worker_name]:

                # Make request to create node
                success = self.request_node_creation(worker_name, node_name)

                # If fail too many times, give up :(
                if not success:
                    logger.error(f"Couldn't create {node_name} after multiple tries")
                    return False

        return True

    def setup_p2p_connections(self) -> bool:
        """Setting up the connections between p2p nodes

        Returns:
            bool: Success in creating the connections
        """

        # After all nodes have been created, get the entire graphs
        # host and port information
        self.nodes_server_table = {}

        # Broadcast request for node server data
        if self.request_node_server_data():

            # Distribute the entire graph's information to all the Workers
            if self.request_connection_creation():
                return True

        return False

    ####################################################################
    ## Package and Dependency Management
    ####################################################################

    def distribute_packages(self, packages_meta: List[Dict[str, Any]]) -> bool:
        """Distribute packages to Workers

        Args:
            packages_meta (List[Dict[str, Any]]): A specification of the \
            package. It's a list of packages where each element is a \
            configuration of the package, using a Dictionary with the \
            following keys: ``name`` and ``path``.

        Returns:
            bool: Success in cluster's workers loading the distributed \
                package.

        """
        # For each package requested, send the packages to be used
        # by the connected Workers.
        for package_meta in packages_meta:
            package_name = package_meta["name"]
            package_path = package_meta["path"]

            # We do so by sending compressed zip files of the packages
            zip_package_dst = self.tempfolder / f"{package_name}.zip"
            with zipfile.PyZipFile(str(zip_package_dst), mode="w") as zip_pkg:
                zip_pkg.writepy(package_path)

            # Send it to the workers and let them know to load the
            # send package
            for worker_name in self.workers:

                worker_data = self.workers[worker_name]

                # Create a temporary HTTP client
                client = Client(
                    self.name,
                    host=worker_data["http_ip"],
                    port=worker_data["http_port"],
                )
                client.send_file(sender_name=self.name, filepath=zip_package_dst)

        # Send package finish confirmation
        self.server.broadcast(
            signal=MANAGER_MESSAGE.REQUEST_CODE_LOAD,
            data={"packages": [x["name"] for x in packages_meta]},
        )

        # Wail until all workers have responded with their node server data
        success = False
        for worker_name in self.workers:

            for i in range(config.get("manager.allowed-failures")):

                success = waiting_for(
                    condition=lambda: self.workers[worker_name]["package_loaded"]
                    == True,
                    msg=f"{self}: Worker {worker_name} loaded package, attempt: {i}",
                )

                if success:
                    break

            if not success:
                break

        # If all workers pass, then yay!
        return success

    ####################################################################
    ## User API
    ####################################################################

    def commit_graph(
        self,
        graph: Graph,
        mapping: Dict[str, List[str]],
        send_packages: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
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

        Args:
            graph (cp.Graph): The graph to deploy within the cluster.
            mapping (Dict[str, List[str]): Mapping from ``cp.Worker`` to\
                ``cp.Nodes`` through a dictionary. The keys are the name\
                of the workers, while the value is a list of the nodes' \
                names.
            send_packages (Optional[List[Dict[str, Any]]]): An optional
                feature for transferring a local package (typically a \
                development package not found via PYPI or Anaconda). \
                Provide a list of packages with each package configured \
                via dictionary with the following key-value pairs: \
                name:``str`` and path:``pathlit.Path``.

        Returns:
            bool: Success in cluster's setup

        """
        # First, test that the graph and the mapping are valid
        self.register_graph(graph)
        self.map_graph(mapping)

        # Then send requested packages
        distribute_package_success = True
        if send_packages:
            distribute_package_success = self.distribute_packages(send_packages)

        # First, create the network
        if distribute_package_success and self.create_p2p_network():

            # Then setup the p2p connections between nodes
            if self.setup_p2p_connections():

                # Wait until the cluster has been setup
                if self.wait_until_all_nodes_ready():
                    return True

        return False

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
        self.server.broadcast(signal=MANAGER_MESSAGE.REQUEST_STEP, data={})

    def gather(self) -> Dict:

        # First, the step should only be possible after a graph has
        # been commited
        assert self.check_all_nodes_ready(), "Manager cannot gather if Nodes not ready"

        # Mark workers as not responded yet
        self.mark_response_as_false_for_workers()

        # Request gathering the latest value of each node
        self.server.broadcast(signal=MANAGER_MESSAGE.REQUEST_GATHER, data={})

        # Wail until all workers have responded with their node server data
        for worker_name in self.workers:

            success = False
            for i in range(config.get("manager.allowed-failures")):

                success = waiting_for(
                    condition=lambda: self.workers[worker_name]["gather"],
                    timeout=config.get("manager.timeout.gather"),
                    msg=f"Worker: {worker_name}: gathering",
                )

                if success:
                    break

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
        # Mark the start time
        self.start_time = datetime.datetime.now()

        # Tell the cluster to start
        self.server.broadcast(signal=MANAGER_MESSAGE.START_NODES, data={})

    def stop(self):
        """Stop the executiong of the cluster.

        Do not forget that you still need to execute ``shutdown`` to
        properly shutdown processes, threads, and queues.

        """
        # Mark the start time
        self.stop_time = datetime.datetime.now()
        self.duration = (self.stop_time - self.start_time).total_seconds()

        # Tell the cluster to stop
        self.server.broadcast(signal=MANAGER_MESSAGE.STOP_NODES, data={})

    def collect(self, unzip: bool = True) -> bool:
        """Collect data from the Workers

        First, we wait until all the Nodes have finished save their data.\
        Then, manager request that Nodes' from the Workers.

        Args:
            unzip (bool): Should the .zip archives be extracted.

        Returns:
            bool: Success in collect data from Workers

        """
        # Wait until the nodes first finished writing down the data
        for worker_name in self.workers:

            success = False
            for i in range(config.get("manager.allowed-failures")):

                # Waiting for completion
                success = waiting_for(
                    condition=self.check_all_nodes_saved,
                    msg=f"Worker {worker_name} waiting for Nodes to be saved, attempt {i}",
                    timeout=max(10, self.duration),
                )

                if success:
                    break

            if not success:
                return False

        # Request
        self.server.broadcast(
            signal=MANAGER_MESSAGE.REQUEST_COLLECT, data={"path": str(self.logdir)}
        )

        # Collect from each worker individually
        for worker_name in self.workers:

            success = False
            for i in range(config.get("manager.allowed-failures")):

                # Waiting for completion
                success = waiting_for(
                    condition=lambda: self.workers[worker_name]["collection_complete"],
                    msg=f"Worker {worker_name} sending data for collection, attempt {i}",
                    timeout=self.duration,
                )

                if success:
                    break
                else:
                    time.sleep(config.get("manager.retry.data-collection"))

            if not success:
                return False

        # # Then move the tempfiles to the log runs and unzip
        self.server.move_transfer_files(self.logdir, unzip)
        logger.info(f"{self}: Data collection complete!")

        # Add the meta data to the archive
        self.save_meta()

        # Success!
        return True

    def shutdown(self):
        """Proper shutting down ChimeraPy cluster.

        Through this method, the ``Manager`` broadcast to all ``Workers``
        to shutdown, in which they will stop their processes and threads safely.

        """
        # Only let shutdown happen once
        if self.has_shutdown:
            logger.debug(f"{self}: requested to shutdown twice, skipping.")
            return None
        else:
            self.has_shutdown = True

        # If workers are connected, let's notify them that the cluster is
        # shutting down
        if len(self.workers) > 0:

            # Send shutdown message
            self.server.broadcast(signal=MANAGER_MESSAGE.CLUSTER_SHUTDOWN, data={})

            # Wait until all worker's deregister
            waiting_for(
                condition=lambda: len(self.workers) == 0,
                check_period=0.1,
                msg=f"{self}: All workers shutdown",
                timeout_raise=False,
                timeout=config.get("manager.timeout.worker-shutdown"),
            )

        # First, shutdown server
        self.server.shutdown()

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
