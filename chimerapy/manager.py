from typing import Dict, Optional, List, Union, Any, Literal
import pickle
import pathlib
import os
import time
import datetime
import json
import random
import tempfile
import zipfile
import concurrent.futures

# Third-party Imports
import dill
import aiohttp
from aiohttp import web
import networkx as nx
import requests

from chimerapy import config
from .networking import Server, Client, DataChunk
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
        port: int = 9000,
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
            routes=[
                web.post("/workers/register", self.register_worker),
                web.post("/workers/node_status", self.update_nodes_status),
            ],
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

    async def register_worker(self, request: web.Request):
        msg = await request.json()

        if msg["register"]:
            self.workers[msg["name"]] = {
                "addr": msg["addr"],
                "http_port": msg["http_port"],
                "http_ip": msg["http_ip"],
                "url": f"http://{msg['http_ip']}:{msg['http_port']}",
                "reported_nodes_server_data": False,
                "served_nodes_server_data": False,
                "nodes_status": {},
                "saving_complete": False,
                "collection_complete": False,
                "package_loaded": False,
            }
            logger.info(
                f"Manager registered <Worker name={msg['name']}> from {msg['addr']}"
            )

        else:
            logger.info(
                f"Manager deregistered <Worker name={msg['name']}> from {msg['addr']}"
            )
            del self.workers[msg["name"]]

        return web.json_response(config.config)

    async def update_nodes_status(self, request: web.Request):
        msg = await request.json()

        # Updating nodes status
        self.workers[msg["name"]]["nodes_status"] = msg["nodes_status"]
        self.workers[msg["name"]]["response"] = True

        logger.debug(f"{self}: Nodes status update to: {self.workers}")

        return web.HTTPOk()

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

    def request_node_creation(self, worker_name: str, node_name: str) -> bool:
        """Request creating a Node from the Graph.

        Args:
            worker_name (str): The targetted Worker
            node_name (str): The name of the node to create, that is in\
            in the graph

        Returns:
            bool: Success in creating the Node

        """
        # Send request to create node
        if isinstance(self.worker_graph_map, nx.Graph):
            logger.warning(f"Cannot create Node {node_name} with Worker {worker_name}")
            return False

        # Send for the creation of the node
        r = requests.post(
            self.workers[worker_name]["url"] + "/nodes/create",
            pickle.dumps(
                {
                    "worker_name": worker_name,
                    "node_name": node_name,
                    "pickled": dill.dumps(
                        self.graph.G.nodes[node_name]["object"], recurse=True
                    ),
                    "in_bound": list(self.graph.G.predecessors(node_name)),
                    "out_bound": list(self.graph.G.successors(node_name)),
                    "follow": self.graph.G.nodes[node_name]["follow"],
                }
            ),
            timeout=config.get("manager.timeout.node-creation"),
        )

        # Wait until the node has been created and connected
        # to the corresponding worker

        if r.status_code == requests.codes.ok:
            data = r.json()
            if data["success"]:
                logger.debug(
                    f"{self}: Node creation ({worker_name}, {node_name}): SUCCESS"
                )
                self.workers[worker_name]["nodes_status"] = data["nodes_status"]
                return True

            else:
                logger.error(
                    f"{self}: Node creation ({worker_name}, {node_name}): FAILED"
                )
                return False

        else:
            logger.error(f"{self}: Worker {worker_name} didn't respond!")
            return False

    def request_node_server_data(self) -> bool:
        """Request Workers to provide information about Node's PUBs

        Returns:
            bool: Success of obtaining the node server data

        """
        # Send the message to each worker and wait
        for worker_name in self.worker_graph_map:
            for i in range(config.get("manager.allowed-failures")):

                # Send the request to each worker
                r = requests.get(
                    self.workers[worker_name]["url"] + "/nodes/server_data",
                )

                if r.status_code == requests.codes.ok:

                    # And then updating the node server data
                    node_server_data = r.json()["node_server_data"]["nodes"]
                    self.nodes_server_table.update(node_server_data)

                    logger.debug(
                        f"{self}: Requesting Worker's node server request: SUCCESS"
                    )
                    break

                else:
                    logger.error(
                        f"{self}: Requesting Worker's node server request: NO RESPONSE"
                    )

        return True

    def request_connection_creation(self) -> bool:
        """Request establishing the connections between Nodes

        This routine the Manager sends the Node's server data and request \
        for Workers to organize their own nodes.

        Returns:
            bool: Returns if connection creation was successful
        """

        # Send the message to each worker and wait
        for worker_name in self.worker_graph_map:

            success = False
            for i in range(config.get("manager.allowed-failures")):

                # Send the request to each worker
                r = requests.post(
                    self.workers[worker_name]["url"] + "/nodes/server_data",
                    json.dumps(self.nodes_server_table),
                    timeout=config.get("manager.timeout.info-request"),
                )

                if r.status_code == requests.codes.ok:
                    data = r.json()
                    if data["success"]:
                        self.workers[worker_name]["nodes_status"] = data["nodes_status"]

                        logger.debug(
                            f"{self}: receiving Worker's node server request: SUCCESS"
                        )
                        success = True
                    break

            if not success:
                logger.error(f"{self}: receiving Worker's node server request: FAILED")
                return False

        return True

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

    def broadcast_request(
        self, htype: Literal["get", "post"], route: str, data: Any = {}
    ) -> bool:

        # Broadcast via a ThreadPool
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self.workers)
        ) as executor:

            def request_start(url):
                if htype == "post":
                    r = requests.post(
                        url + route,
                        json.dumps(data),
                        timeout=config.get("manager.timeout.info-request"),
                    )
                elif htype == "get":
                    r = requests.get(
                        url + route,
                        json.dumps(data),
                        timeout=config.get("manager.timeout.info-request"),
                    )
                return r, url

            futures = (
                executor.submit(request_start, worker["url"])
                for worker in self.workers.values()
            )
            success = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    r, url = future.result()
                    if r.status_code == requests.codes.ok:
                        logger.debug(f"{self}: Worker {url + route}: SUCCESS")
                        success.append(True)
                    else:
                        logger.error(f"{self}: Worker {url + route}: FAILED, retry")
                        success.append(False)

                except Exception as exc:
                    logger.error(exc)
                    success.append(False)

        return all(success)

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

        # Wail until all workers have responded with their node server data
        success = False
        for worker_name in self.workers:

            for i in range(config.get("manager.allowed-failures")):

                # Send package finish confirmation
                r = requests.post(
                    self.workers[worker_name]["url"] + "/packages/load",
                    json.dumps({"packages": [x["name"] for x in packages_meta]}),
                    timeout=config.get("manager.timeout.package-delivery"),
                )

                if r.status_code == requests.codes.ok:
                    logger.debug(
                        f"{self}: Worker {worker_name} loaded package: SUCCESS"
                    )
                    success = True
                    break

            if not success:
                logger.error(f"{self}: Worker {worker_name} loaded package: FAILED")
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

        # If package are sent correctly, try to create network
        # Start with the nodes and then the connections
        if (
            distribute_package_success
            and self.create_p2p_network()
            and self.setup_p2p_connections()
        ):
            return True

        return False

    def step(self) -> bool:
        """Cluster step execution for offline operation.

        The ``step`` function is for careful but slow operation of the
        cluster. For online execution, ``start`` and ``stop`` are the
        methods to be used.

        Returns:
            bool: Success of step function broadcasting

        """
        return self.broadcast_request("post", "/nodes/step")

    def gather(self) -> Dict:

        # Wail until all workers have responded with their node server data
        gather_data = {}
        for worker_name in self.workers:

            r = requests.get(
                self.workers[worker_name]["url"] + "/nodes/gather",
                timeout=config.get("manager.timeout.info-request"),
            )
            logger.debug(r)

            if r.status_code == requests.codes.ok:
                logger.debug(f"Worker: {worker_name}, gathering: SUCCESS")
            else:
                logger.error(f"Worker: {worker_name}, gathering: FAILED")

            # Saving the data
            data = pickle.loads(r.content)["node_data"]
            for node_name, node_data in data.items():
                data[node_name] = DataChunk.from_bytes(node_data)

            gather_data.update(data)

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
        return self.broadcast_request("post", "/nodes/start")

    def stop(self):
        """Stop the executiong of the cluster.

        Do not forget that you still need to execute ``shutdown`` to
        properly shutdown processes, threads, and queues.

        """
        # Mark the start time
        self.stop_time = datetime.datetime.now()
        self.duration = (self.stop_time - self.start_time).total_seconds()

        # Tell the cluster to start
        return self.broadcast_request("post", "/nodes/stop")

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
        success = self.broadcast_request("post", "/nodes/save")
        if success:
            for worker_name in self.workers:
                for node_name in self.workers[worker_name]["nodes_status"]:
                    self.workers[worker_name]["nodes_status"][node_name]["FINISHED"] = 1

        # Request collecting archives
        success = self.broadcast_request(
            "post", "/nodes/collect", {"path": str(self.logdir)}
        )

        # # Then move the tempfiles to the log runs and unzip
        self.server.move_transfer_files(self.logdir, unzip)
        logger.info(f"{self}: Data collection complete!")

        # Add the meta data to the archive
        self.save_meta()

        # Success!
        return success

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
            try:
                self.broadcast_request("post", "/shutdown")
            except:
                pass

            # Wait until all worker's deregister
            success = waiting_for(
                condition=lambda: len(self.workers) == 0,
                check_period=0.1,
                timeout_raise=False,
                timeout=config.get("manager.timeout.worker-shutdown"),
            )

            if success:
                logger.debug(f"{self}: All workers shutdown: SUCCESS")
            else:
                logger.warning(f"{self}: All workers shutdown: FAILED - forced")

        # First, shutdown server
        self.server.shutdown()

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
