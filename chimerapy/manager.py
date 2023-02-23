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
from concurrent.futures import Future

# Third-party Imports
import dill
import aiohttp
from aiohttp import web
import networkx as nx
import requests

from chimerapy import config
from .states import ManagerState, WorkerState, NodeState
from .networking import Server, Client, DataChunk
from .graph import Graph
from .exceptions import CommitGraphError
from . import _logger
from .utils import waiting_for

logger = _logger.getLogger("chimerapy")


class Manager:
    def __init__(
        self,
        logdir: Union[pathlib.Path, str],
        port: int = 9000,
        max_num_of_workers: int = 50,
        publish_logs_via_zmq: bool = False,
        dashboard_api: bool = True,
        **kwargs,
    ):
        """Create ``Manager``, the controller of the cluster.

        The ``Manager`` is the director of the cluster, such as adding
        new computers, providing roles, starting and stopping data
        collection, and shutting down the system.

        Args:
            port (int): Referred port, might return a different one based\
            on availablity.
            max_num_of_workers (int): max_num_of_workers
            publish_logs_via_zmq (bool, optional): Whether to publish logs via ZMQ. Defaults to False.
            dashboard_api (bool): Enable front-end API entrypoints to controll cluster. Defaults to True.
            **kwargs: Additional keyword arguments.
                Currently, this is used to configure the ZMQ log handler.
        """
        # Saving input parameters
        self.max_num_of_workers = max_num_of_workers
        self.has_shutdown = False

        # Create log directory to store data
        timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        rand_num = random.randint(1000, 9999)
        self.logdir = (
            pathlib.Path(logdir).resolve() / f"chimerapy-{timestamp}-{rand_num}"
        )

        # Also create a tempfolder to store any miscellaneous files and folders
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        # Create a logging directory
        os.makedirs(self.logdir, exist_ok=True)

        # Instance variables
        self.graph: Graph = Graph()
        self.worker_graph_map: Dict = {}
        self.commitable_graph: bool = False
        self.nodes_server_table: Dict = {}
        self.start_time: Optional[datetime.datetime] = None
        self.stop_time: Optional[datetime.datetime] = None
        self.duration: int = 0

        if publish_logs_via_zmq:
            handler_config = _logger.ZMQLogHandlerConfig.from_dict(kwargs)
            _logger.add_zmq_handler(logger, handler_config)

        # Create server
        self.server = Server(
            port=port,
            id="Manager",
            routes=[
                web.post("/workers/register", self.register_worker),
                web.post("/workers/deregister", self.deregister_worker),
                web.post("/workers/node_status", self.update_nodes_status),
            ],
        )

        # Enable Dashboard API if requested (needed to be executed after server is running)
        if dashboard_api:
            from .api import API

            self.dashboard_api = API(self)

        # Runn the Server
        self.server.serve()
        logger.info(f"Manager started at {self.server.host}:{self.server.port}")

        # Updating the manager's port to the found available port
        ip, port = self.server.host, self.server.port
        self.state = ManagerState(id="Manager", ip=ip, port=port)

    def __repr__(self):
        return f"<Manager @{self.host}:{self.port}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Properties
    ####################################################################

    @property
    def host(self) -> str:
        return self.state.ip

    @property
    def port(self) -> int:
        return self.state.port

    @property
    def workers(self) -> Dict[str, WorkerState]:
        return self.state.workers

    ####################################################################
    ## Worker -> Manager Messages
    ####################################################################

    async def register_worker(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        logger.debug(worker_state)

        self.state.workers[worker_state.id] = worker_state
        logger.info(
            f"Manager registered <Worker id={worker_state.id} name={worker_state.name}> from {worker_state.ip}"
        )
        logger.debug(f"{self}: WorkerState: {self.state.workers}")

        return web.json_response(config.config)

    async def deregister_worker(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        logger.info(
            f"Manager deregistered <Worker id={worker_state.id} name={worker_state.name}> from {worker_state.ip}"
        )

        if worker_state.id in self.state.workers:
            del self.state.workers[worker_state.id]
            return web.HTTPOk()
        else:
            return web.HTTPBadRequest()

    async def update_nodes_status(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Updating nodes status
        self.state.workers[worker_state.id] = worker_state
        logger.debug(f"{self}: Nodes status update to: {self.state.workers}")

        # Relay information to front-end
        await self.dashboard_api.broadcast_node_update()

        return web.HTTPOk()

    ####################################################################
    ## Helper Methods (Cluster)
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
            "workers": list(self.state.workers.keys()),
            "nodes": list(self.graph.G.nodes()),
            "worker_graph_map": self.worker_graph_map,
            "nodes_server_table": self.nodes_server_table,
            "start_time": start_time,
            "stop_time": stop_time,
        }

        with open(self.logdir / "meta.json", "w") as f:
            json.dump(meta, f, indent=2)

    def request_node_creation(self, worker_id: str, node_id: str) -> bool:
        """Request creating a Node from the Graph.

        Args:
            worker_id (str): The targetted Worker
            node_id (str): The id of the node to create, that is in\
            in the graph

        Returns:
            bool: Success in creating the Node

        """
        # Send request to create node
        if isinstance(self.worker_graph_map, nx.Graph):
            logger.warning(f"Cannot create Node {node_id} with Worker {worker_id}")
            return False

        # Extract the in_bound list and provide also the node's names
        in_bound = list(self.graph.G.predecessors(node_id))
        node_data = self.graph.G.nodes(data=True)
        in_bound_by_name = [node_data[x]["object"].name for x in in_bound]

        # Send for the creation of the node
        r = requests.post(
            f"http://{self.state.workers[worker_id].ip}:{self.state.workers[worker_id].port}"
            + "/nodes/create",
            pickle.dumps(
                {
                    "worker_id": worker_id,
                    "id": node_id,
                    "pickled": dill.dumps(
                        self.graph.G.nodes[node_id]["object"], recurse=True
                    ),
                    "in_bound": in_bound,
                    "in_bound_by_name": in_bound_by_name,
                    "out_bound": list(self.graph.G.successors(node_id)),
                    "follow": self.graph.G.nodes[node_id]["follow"],
                }
            ),
            timeout=config.get("manager.timeout.node-creation"),
        )

        # Wait until the node has been created and connected
        # to the corresponding worker

        if r.status_code == requests.codes.ok:
            data = r.json()
            if data["success"]:
                logger.debug(f"{self}: Node creation ({worker_id}, {node_id}): SUCCESS")
                self.state.workers[worker_id].nodes[node_id] = NodeState.from_dict(
                    data["node_state"]
                )
                logger.debug(f"{self}: WorkerState: {self.state.workers}")
                return True

            else:
                logger.error(f"{self}: Node creation ({worker_id}, {node_id}): FAILED")
                return False

        else:
            logger.error(f"{self}: Worker {worker_id} didn't respond!")
            return False

    def request_node_server_data(self, worker_id: str) -> bool:
        """Request Workers to provide information about Node's PUBs

        Returns:
            bool: Success of obtaining the node server data

        """
        for i in range(config.get("manager.allowed-failures")):

            # Send the request to each worker
            r = requests.get(
                f"http://{self.state.workers[worker_id].ip}:{self.state.workers[worker_id].port}"
                + "/nodes/server_data",
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

    def request_connection_creation(self, worker_id: str) -> bool:
        """Request establishing the connections between Nodes

        This routine the Manager sends the Node's server data and request \
        for Workers to organize their own nodes.

        Returns:
            bool: Returns if connection creation was successful
        """

        # Send the message to each worker and wait
        success = False
        for i in range(config.get("manager.allowed-failures")):

            # Send the request to each worker
            r = requests.post(
                f"http://{self.state.workers[worker_id].ip}:{self.state.workers[worker_id].port}"
                + "/nodes/server_data",
                json.dumps(self.nodes_server_table),
                timeout=config.get("manager.timeout.info-request"),
            )

            if r.status_code == requests.codes.ok:
                data = r.json()
                if data["success"]:
                    self.state.workers[worker_id] = WorkerState.from_dict(
                        data["worker_state"]
                    )
                    logger.debug(f"{self}: WorkerState: {self.state.workers}")

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
        for worker_id in worker_graph_map:

            # First check if the worker is registered!
            if worker_id not in self.state.workers:
                logger.error(f"{worker_id} is not register to Manager.")
                checks.append(False)
                break
            else:
                checks.append(True)

            # Then check if the node exists in the graph
            for node_id in worker_graph_map[worker_id]:
                if not self.graph.has_node_by_id(node_id):
                    logger.error(f"{node_id} is not in Manager's graph.")
                    checks.append(False)
                    break
                else:
                    checks.append(True)

        if not all(checks):
            raise CommitGraphError("Mapping is invalid")

        # Save the worker graph
        self.worker_graph_map = worker_graph_map

    def broadcast_request(
        self,
        htype: Literal["get", "post"],
        route: str,
        data: Any = {},
        timeout: Union[int, float] = config.get("manager.timeout.info-request"),
    ) -> bool:

        # Broadcast via a ThreadPool
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self.state.workers) + 1
        ) as executor:

            def request_start(url):
                if htype == "post":
                    r = requests.post(url + route, json.dumps(data), timeout=timeout)
                elif htype == "get":
                    r = requests.get(url + route, json.dumps(data), timeout=timeout)
                return r, url

            futures = (
                executor.submit(request_start, f"http://{worker.ip}:{worker.port}")
                for worker in self.state.workers.values()
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
        futures: List[Future] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.get("manager.misc.num-of-threads")
        ) as executor:
            for worker_id in self.worker_graph_map:
                for node_id in self.worker_graph_map[worker_id]:

                    # Make request to create node
                    futures.append(
                        executor.submit(self.request_node_creation, worker_id, node_id)
                    )

        success = []
        for future in concurrent.futures.as_completed(futures):
            try:
                success.append(future.result())
            except Exception as exc:
                logger.error(exc)
                success.append(False)

        return all(success)

    def setup_p2p_connections(self) -> bool:
        """Setting up the connections between p2p nodes

        Returns:
            bool: Success in creating the connections
        """

        # After all nodes have been created, get the entire graphs
        # host and port information
        self.nodes_server_table = {}

        # Broadcast request for node server data
        futures: List[Future] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.get("manager.misc.num-of-threads")
        ) as executor:
            for worker_id in self.worker_graph_map:

                # Make the request
                futures.append(
                    executor.submit(self.request_node_server_data, worker_id)
                )

        success = []
        for future in concurrent.futures.as_completed(futures):
            try:
                success.append(future.result())
            except Exception as exc:
                logger.error(exc)
                success.append(False)

        # If all success, then continue making the connection
        if all(success):

            # Distribute the entire graph's information to all the Workers
            futures: List[Future] = []
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=config.get("manager.misc.num-of-threads")
            ) as executor:
                for worker_id in self.worker_graph_map:

                    futures.append(
                        executor.submit(self.request_connection_creation, worker_id)
                    )

            success = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    success.append(future.result())
                except Exception as exc:
                    logger.error(exc)
                    success.append(False)

        return all(success)

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
            for worker_id, worker_data in self.state.workers.items():

                # Create a temporary HTTP client
                client = Client(
                    self.state.id,
                    host=worker_data.ip,
                    port=worker_data.port,
                )
                client.send_file(sender_id=self.state.id, filepath=zip_package_dst)

        # Wail until all workers have responded with their node server data
        success = False
        for worker_id in self.state.workers:

            for i in range(config.get("manager.allowed-failures")):

                # Send package finish confirmation
                r = requests.post(
                    f"http://{self.state.workers[worker_id].ip}:{self.state.workers[worker_id].port}"
                    + "/packages/load",
                    json.dumps({"packages": [x["name"] for x in packages_meta]}),
                    timeout=config.get("manager.timeout.package-delivery"),
                )

                if r.status_code == requests.codes.ok:
                    logger.debug(f"{self}: Worker {worker_id} loaded package: SUCCESS")
                    success = True
                    break

            if not success:
                logger.error(f"{self}: Worker {worker_id} loaded package: FAILED")
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
        for worker_id in self.state.workers:

            r = requests.get(
                f"http://{self.state.workers[worker_id].ip}:{self.state.workers[worker_id].port}"
                + "/nodes/gather",
                timeout=config.get("manager.timeout.info-request"),
            )
            logger.debug(r)

            if r.status_code == requests.codes.ok:
                logger.debug(f"Worker: {worker_id}, gathering: SUCCESS")
            else:
                logger.error(f"Worker: {worker_id}, gathering: FAILED")

            # Saving the data
            data = pickle.loads(r.content)["node_data"]
            for node_id, node_data in data.items():
                data[node_id] = DataChunk.from_json(node_data)

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
            for worker_id in self.state.workers:
                for node_id in self.state.workers[worker_id].nodes:
                    self.state.workers[worker_id].nodes[node_id].finished = True

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

        logger.debug(f"{self}: shutting down")

        # If workers are connected, let's notify them that the cluster is
        # shutting down
        if len(self.state.workers) > 0:

            # Send shutdown message
            logger.debug(f"{self}: broadcasting shutdown via /shutdown route")
            try:
                self.broadcast_request(
                    "post",
                    "/shutdown",
                    timeout=config.get("manager.timeout.worker-shutdown"),
                )
            except:
                pass
            logger.debug(
                f"{self}: requested all workers to shutdown via /shutdown route"
            )

            # Wait until all worker's deregister
            success = waiting_for(
                condition=lambda: len(self.state.workers) == 0,
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
