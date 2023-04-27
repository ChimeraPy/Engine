from typing import Dict, Optional, List, Union, Any, Literal, Coroutine
import pickle
import asyncio
import pathlib
import os
import datetime
import json
import random
import tempfile
import zipfile
import traceback
import socket
from concurrent.futures import Future

# Third-party Imports
import dill
import aiohttp
from aiohttp import web
import networkx as nx
from zeroconf import ServiceInfo, Zeroconf

from chimerapy import config
from .states import ManagerState, WorkerState, NodeState
from .networking import Server, Client, DataChunk
from .networking.enums import MANAGER_MESSAGE, GENERAL_MESSAGE
from .graph import Graph
from .exceptions import CommitGraphError
from . import _logger
from .logger.distributed_logs_sink import DistributedLogsMultiplexedFileSink
from .utils import megabytes_to_bytes

logger = _logger.getLogger("chimerapy")


class Manager:
    def __init__(
        self,
        logdir: Union[pathlib.Path, str],
        port: int = 9000,
        max_num_of_workers: int = 50,
        publish_logs_via_zmq: bool = False,
        enable_api: bool = True,
        enable_zeroconf: bool = True,
        **kwargs,
    ):
        """Create ``Manager``, the controller of the cluster.

        The ``Manager`` is the director of the cluster, such as adding
        new computers, providing roles, starting and stopping data
        collection, and shutting down the system.

        Args:
            port (int): Referred port, might return a different one based\
                on availablity.
            max_num_of_workers (int): Maximum number of allowed Workers
            publish_logs_via_zmq (bool, optional): Whether to publish logs via ZMQ. \
                Defaults to False.
            enable_api (bool): Enable front-end API entrypoints to controll cluster. \
                Defaults to True.
            enable_zeroconf (bool): Enable the Zeroconf connection method for Workers. \
                Defauls to True.

            **kwargs: Additional keyword arguments.
                Currently, this is used to configure the ZMQ log handler.
        """
        # Saving input parameters
        self.max_num_of_workers = max_num_of_workers
        self.has_shutdown = False
        self.enable_api = enable_api
        self.enable_zeroconf = enable_zeroconf

        # Create log directory to store data
        timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        rand_num = random.randint(1000, 9999)
        self.logdir = (
            pathlib.Path(logdir).resolve() / f"chimerapy-{timestamp}-{rand_num}"
        )

        # Creating a container for task futures
        self.task_futures: List[Future] = []

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
                web.post("/workers/register", self._register_worker_route),
                web.post("/workers/deregister", self._deregister_worker_route),
                web.post("/workers/node_status", self._update_nodes_status),
            ],
        )

        # Enable Dashboard API if requested
        if self.enable_api:
            from .api import API

            self.dashboard_api = API(self)

        # Runn the Server
        self.server.serve()
        logger.info(f"Manager started at {self.server.host}:{self.server.port}")

        # After creating the server and if enabled, setup zeroconf
        if self.enable_zeroconf:

            # Create service information
            self.zeroconf_info = ServiceInfo(
                "_http._tcp.local.",
                f"chimerapy-{rand_num}._http._tcp.local.",
                addresses=[socket.inet_aton(self.server.host)],
                port=self.server.port,
                properties={"path": str(self.logdir), "timestamp": timestamp},
            )

            # Start Zeroconf Service
            self.zeroconf = Zeroconf()
            self.zeroconf.register_service(self.zeroconf_info, ttl=60)
            logger.info(
                f"Manager started Zeroconf Service named \
                chimerapy-{rand_num}._http._tcp.local."
            )

        # Updating the manager's port to the found available port
        ip, port = self.server.host, self.server.port
        self.state = ManagerState(id="Manager", ip=ip, port=port)

        if config.get("manager.logs-sink.enabled"):
            self.logs_sink = self._start_logs_sink()
        else:
            self.logs_sink = None

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
    ## Worker -> Manager HTTP Messages
    ####################################################################

    async def _register_worker_route(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Register worker
        success = self._register_worker(worker_state)

        if success:
            logs_collection_info = {
                "enabled": self.logs_sink is not None,
                "host": self.host if self.logs_sink else None,
                "port": self.logs_sink.port if self.logs_sink else None,
            }

            response = {
                "logs_push_info": logs_collection_info,
                "config": config.config,
            }

            await self._broadcast_network_status_update()
            return web.json_response(response)

        else:
            return web.HTTPError()

    async def _deregister_worker_route(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)
        success = self._deregister_worker(worker_state.id)

        if success:
            await self._broadcast_network_status_update()
            return web.HTTPOk()
        else:
            return web.HTTPBadRequest()

    async def _update_nodes_status(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Updating nodes status
        self.state.workers[worker_state.id] = worker_state
        logger.debug(f"{self}: Nodes status update to: {self.state.workers}")

        # Relay information to front-end
        if self.enable_api:
            await self.dashboard_api.broadcast_state_update()

        return web.HTTPOk()

    ####################################################################
    ## Utils Methods
    ####################################################################

    def _save_meta(self):
        # Get the times, handle Optional
        if self.start_time:
            start_time = self.start_time.strftime("%Y_%m_%d_%H_%M_%S.%f%z")
        else:
            start_time = None

        if self.stop_time:
            stop_time = self.stop_time.strftime("%Y_%m_%d_%H_%M_%S.%f%z")
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

    def _exec_coro(self, coro: Coroutine) -> Future:
        # Submitting the coroutine
        future = self.server._thread.exec(coro)

        # Saving the future for later use
        self.task_futures.append(future)

        return future

    def _register_worker_to_logs_sink(self, worker_name: str, worker_id: str):
        if not self.logdir.exists():
            self.logdir.mkdir(parents=True)
        self.logs_sink.initialize_entity(worker_name, worker_id, self.logdir)
        logger.info(f"Registered worker {worker_name} to logs sink")

    @staticmethod
    def _start_logs_sink() -> DistributedLogsMultiplexedFileSink:
        """Start the logs sink."""
        max_bytes_per_worker = megabytes_to_bytes(
            config.get("manager.logs-sink.max-file-size-per-worker")
        )
        logs_sink = _logger.get_distributed_logs_multiplexed_file_sink(
            max_bytes=max_bytes_per_worker
        )
        logs_sink.start(register_exit_handlers=True)
        return logs_sink

    def _get_worker_ip(self, worker_id: str) -> str:
        worker_info = self.state.workers[worker_id]
        return f"http://{worker_info.ip}:{worker_info.port}"

    def _clear_node_server(self):
        self.nodes_server_table: Dict = {}

    def _register_worker(self, worker_state: WorkerState) -> bool:

        self.state.workers[worker_state.id] = worker_state
        logger.info(
            f"Manager registered <Worker id={worker_state.id} \
            name={worker_state.name}> from {worker_state.ip}"
        )
        logger.debug(f"{self}: WorkerState: {self.state.workers}")

        if self.logs_sink is not None:
            self._register_worker_to_logs_sink(
                worker_name=worker_state.name, worker_id=worker_state.id
            )

        return True

    def _deregister_worker(self, worker_id: str) -> bool:

        if self.logs_sink is not None:
            self.logs_sink.deregister_entity(worker_id)

        if worker_id in self.state.workers:
            state = self.state.workers[worker_id]
            logger.info(
                f"Manager deregistered <Worker id={worker_id} name={state.name}> \
                from {state.ip}"
            )
            del self.state.workers[worker_id]

            return True

        return False

    async def _broadcast_network_status_update(self, is_shutdown: bool = False):
        if self.enable_api:
            logger.debug(f"{self}: Broadcasting network status update")
            await self.dashboard_api.broadcast_state_update(
                signal=MANAGER_MESSAGE.NETWORK_STATUS_UPDATE
                if not is_shutdown
                else GENERAL_MESSAGE.SHUTDOWN
            )

    def _register_graph(self, graph: Graph):
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

    def _deregister_graph(self):
        self.graph: Graph = Graph()

    def _map_graph(self, worker_graph_map: Dict[str, List[str]]):
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

    def node_to_worker_lookup(self, node_id: str) -> Optional[str]:

        for worker_id in self.state.workers:
            if node_id in self.state.workers[worker_id].nodes:
                return worker_id

        logger.error(f"{self}: Node-Worker Lookup failed: {node_id}")
        return None

    ####################################################################
    ## Package and Dependency Management
    ####################################################################

    async def _distribute_packages(self, packages_meta: List[Dict[str, Any]]) -> bool:
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
                await client._send_file_async(
                    url=f"{self._get_worker_ip(worker_id)}/file/post",
                    sender_id=self.state.id,
                    filepath=zip_package_dst,
                )

        # Wail until all workers have responded with their node server data
        success = False
        for worker_id in self.state.workers:

            for i in range(config.get("manager.allowed-failures")):

                url = f"{self._get_worker_ip(worker_id)}/packages/load"
                data = json.dumps({"packages": [x["name"] for x in packages_meta]})

                async with aiohttp.ClientSession() as client:
                    async with client.post(
                        url=url,
                        data=data,
                        timeout=config.get("manager.timeout.package-delivery"),
                    ) as resp:
                        if resp.ok:
                            logger.debug(
                                f"{self}: Worker {worker_id} loaded package: SUCCESS"
                            )
                            success = True
                            break

            if not success:
                logger.error(f"{self}: Worker {worker_id} loaded package: FAILED")
                return False

        # If all workers pass, then yay!
        return success

    ####################################################################
    ## Async Networking
    ####################################################################

    async def _async_request_node_creation(self, worker_id: str, node_id: str) -> bool:
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
        elif node_id not in self.graph.G:
            return False

        logger.debug(f"{self}: Node requested to be created: {worker_id} - {node_id}")

        # Extract the in_bound list and provide also the node's names
        in_bound = list(self.graph.G.predecessors(node_id))
        node_data = self.graph.G.nodes(data=True)
        in_bound_by_name = [node_data[x]["object"].name for x in in_bound]

        # Create the data to be send
        data = pickle.dumps(
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
        )

        # Construct the url
        url = f"{self._get_worker_ip(worker_id)}/nodes/create"
        logger.debug(f"{self}: Sending request to {url}")

        # Send for the creation of the node
        async with aiohttp.ClientSession() as client:
            async with client.post(url=url, data=data) as resp:
                if resp.ok:
                    data = await resp.json()

                    if data["success"]:
                        logger.debug(
                            f"{self}: Node creation ({worker_id}, {node_id}): SUCCESS"
                        )
                        self.state.workers[worker_id].nodes[
                            node_id
                        ] = NodeState.from_dict(data["node_state"])
                        logger.debug(f"{self}: WorkerState: {self.state.workers}")
                        return True

                    else:
                        logger.error(
                            f"{self}: Node creation ({worker_id}, {node_id}): FAILED"
                        )
                        return False

        logger.error(f"{self}: Worker {worker_id} didn't respond!")

        return False

    async def _async_request_node_destruction(
        self, worker_id: str, node_id: str
    ) -> bool:
        """Request destroying a Node from the Graph.

        Args:
            worker_id (str): The targetted Worker
            node_id (str): The id of the node to create, that is in\
            in the graph

        Returns:
            bool: Success in creating the Node

        """
        # Send request to create node
        if isinstance(self.worker_graph_map, nx.Graph):
            logger.warning(f"Cannot destroy Node {node_id} with Worker {worker_id}")
            return False
        elif node_id not in self.graph.G:
            return False

        logger.debug(f"{self}: Node requested to be destroyed: {worker_id} - {node_id}")

        # Construct the url
        url = f"{self._get_worker_ip(worker_id)}/nodes/destroy"
        logger.debug(f"{self}: Sending request to {url}")

        # Create the data to be send
        data = {
            "worker_id": worker_id,
            "id": node_id,
        }

        # Send for the creation of the node
        async with aiohttp.ClientSession() as client:
            async with client.post(url=url, data=json.dumps(data)) as resp:
                if resp.ok:
                    data = await resp.json()

                    if data["success"]:
                        logger.debug(
                            f"{self}: Node destroy ({worker_id}, {node_id}): SUCCESS"
                        )
                        self.state.workers[worker_id] = WorkerState.from_dict(
                            data["worker_state"]
                        )
                        logger.debug(f"{self}: WorkerState: {self.state.workers}")
                        return True

                    else:
                        logger.error(
                            f"{self}: Node destroy ({worker_id}, {node_id}): FAILED"
                        )
                        return False

        logger.error(f"{self}: Worker {worker_id} didn't respond!")

        return False

    async def _async_request_node_server_data(self, worker_id: str) -> bool:
        """Request Workers to provide information about Node's PUBs

        Returns:
            bool: Success of obtaining the node server data

        """
        for i in range(config.get("manager.allowed-failures")):
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    f"{self._get_worker_ip(worker_id)}/nodes/server_data"
                ) as resp:
                    if resp.ok:
                        # Get JSON
                        data = await resp.json()

                        # And then updating the node server data
                        node_server_data = data["node_server_data"]["nodes"]
                        self.nodes_server_table.update(node_server_data)

                        logger.debug(
                            f"{self}: Requesting Worker's node server request: SUCCESS"
                        )
                        return True

                    else:
                        logger.error(
                            f"{self}: Requesting Worker's node server request: NO \
                            RESPONSE"
                        )

        return False

    async def _async_request_connection_creation(self, worker_id: str) -> bool:
        """Request establishing the connections between Nodes

        This routine the Manager sends the Node's server data and request \
        for Workers to organize their own nodes.

        Returns:
            bool: Returns if connection creation was successful
        """

        # Send the message to each worker and wait
        for i in range(config.get("manager.allowed-failures")):

            # Send the request to each worker
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    f"{self._get_worker_ip(worker_id)}/nodes/server_data",
                    data=json.dumps(self.nodes_server_table),
                ) as resp:
                    if resp.ok:
                        # Get JSON
                        data = await resp.json()

                        if data["success"]:
                            self.state.workers[worker_id] = WorkerState.from_dict(
                                data["worker_state"]
                            )
                            logger.debug(f"{self}: WorkerState: {self.state.workers}")

                            logger.debug(
                                f"{self}: receiving Worker's node server request: \
                                SUCCESS"
                            )
                            return True

        return False

    async def _async_broadcast_request(
        self,
        htype: Literal["get", "post"],
        route: str,
        data: Any = {},
        timeout: Optional[Union[int, float]] = config.get(
            "manager.timeout.info-request"
        ),
        report_exceptions: bool = True,
    ) -> bool:

        # Create a new session for the moment
        tasks: List[asyncio.Task] = []
        sessions: List[aiohttp.ClientSession] = []
        for worker_data in self.state.workers.values():
            session = aiohttp.ClientSession()
            url = f"http://{worker_data.ip}:{worker_data.port}" + route
            logger.debug(f"{self}: Broadcasting request to {url}")
            if htype == "get":
                tasks.append(
                    asyncio.create_task(session.get(url, data=json.dumps(data)))
                )
            elif htype == "post":
                tasks.append(
                    asyncio.create_task(session.post(url, data=json.dumps(data)))
                )

            # Storing sessions to later close
            sessions.append(session)

        # Wait with a timeout
        try:
            await asyncio.wait(tasks, timeout=timeout)
        except Exception:

            # Disregard certain exceptions
            logger.error(traceback.format_exc())
            return False

        # Get their outputs
        results: List[bool] = []
        for t in tasks:
            try:
                result = t.result()
                results.append(result.ok)
            except Exception:

                # Disregard certain exceptions
                if report_exceptions:
                    logger.error(traceback.format_exc())
                    return False
                else:
                    results.append(True)

        # Closing sessions
        for session in sessions:
            await session.close()

        # Return if all outputs were successful
        return all(results)

    async def _async_create_p2p_network(self) -> bool:
        """Create the P2P Nodes in the Network

        This routine only creates the Node via the Workers but doesn't \
        establish the connections.

        Returns:
            bool: Success in creating the P2P Nodes

        """
        # Send the message to each worker
        coros: List[Coroutine] = []
        for worker_id in self.worker_graph_map:
            for node_id in self.worker_graph_map[worker_id]:

                # Request the creation
                coro = self._async_request_node_creation(worker_id, node_id)
                coros.append(coro)

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        return all(results)

    async def _async_setup_p2p_connections(self) -> bool:
        """Setting up the connections between p2p nodes

        Returns:
            bool: Success in creating the connections

        """
        # After all nodes have been created, get the entire graphs
        # host and port information
        self.nodes_server_table = {}

        # Broadcast request for node server data
        coros: List[Coroutine] = []
        for worker_id in self.worker_graph_map:
            coros.append(self._async_request_node_server_data(worker_id))

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        if not all(results):
            return False

        # Distribute the entire graph's information to all the Workers
        coros: List[Coroutine] = []
        for worker_id in self.worker_graph_map:
            coros.append(self._async_request_connection_creation(worker_id))

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        return all(results)

    ####################################################################
    ## Sync Networking
    ####################################################################

    def _request_node_creation(self, worker_id: str, node_id: str) -> Future[bool]:
        return self._exec_coro(self._async_request_node_creation(worker_id, node_id))

    def _request_node_destruction(self, worker_id: str, node_id: str) -> Future[bool]:
        return self._exec_coro(self._async_request_node_destruction(worker_id, node_id))

    def _request_node_server_data(self, worker_id: str) -> Future[bool]:
        return self._exec_coro(self._async_request_node_server_data(worker_id))

    def _request_connection_creation(self, worker_id: str) -> Future[bool]:
        return self._exec_coro(self._async_request_connection_creation(worker_id))

    def _broadcast_request(
        self,
        htype: Literal["get", "post"],
        route: str,
        data: Any = {},
        timeout: Union[int, float] = config.get("manager.timeout.info-request"),
    ) -> Future[bool]:
        return self._exec_coro(
            self._async_broadcast_request(htype, route, data, timeout)
        )

    ####################################################################
    ## Front-facing ASync API
    ####################################################################

    async def async_commit(
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
        self._register_graph(graph)
        self._map_graph(mapping)
        self._save_meta()

        # Then send requested packages
        success = True
        if send_packages:
            success = await self._distribute_packages(send_packages)

        # If package are sent correctly, try to create network
        # Start with the nodes and then the connections
        if (
            success
            and await self._async_create_p2p_network()
            and await self._async_setup_p2p_connections()
        ):
            return True

        return False

    async def async_gather(self) -> Dict:
        # Wail until all workers have responded with their node server data
        gather_data = {}
        for worker_id in self.state.workers:

            async with aiohttp.ClientSession() as client:
                async with client.get(
                    f"{self._get_worker_ip(worker_id)}/nodes/gather",
                    timeout=config.get("manager.timeout.info-request"),
                ) as resp:
                    if resp.ok:
                        logger.debug(f"{self}: Gathering Worker {worker_id}: SUCCESS")

                        # Read the content
                        content = await resp.content.read()

                        # Saving the data
                        data = pickle.loads(content)["node_data"]
                        for node_id, node_data in data.items():
                            data[node_id] = DataChunk.from_json(node_data)

                        gather_data.update(data)

                    else:
                        logger.error(f"{self}: Gathering Worker {worker_id}: FAILED")

        return gather_data

    async def async_start(self) -> bool:

        # Mark the start time
        self.start_time = datetime.datetime.now()

        # Tell the cluster to start
        success = await self._async_broadcast_request("post", "/nodes/start")
        if success:
            self.state.running = True

        # Updating meta just in case of failure
        self._save_meta()

        return success

    async def async_record(self) -> bool:

        # Mark the start time
        self.start_time = datetime.datetime.now()

        # Tell the cluster to start
        success = await self._async_broadcast_request("post", "/nodes/record")
        if success:
            self.state.running = True

        # Updating meta just in case of failure
        self._save_meta()

        return success

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:

        # First, identify which worker has the node
        worker_id = self.node_to_worker_lookup(node_id)

        if not isinstance(worker_id, str):
            return {"success": False, "output": None}

        data = json.dumps(
            {
                "node_id": node_id,
                "method_name": method_name,
                "params": params,
            }
        )

        async with aiohttp.ClientSession() as client:
            async with client.post(
                f"{self._get_worker_ip(worker_id)}/nodes/registered_methods",
                timeout=config.get("manager.timeout.info-request"),
                data=data,
            ) as resp:
                if resp.ok:
                    logger.debug(f"{self}: Gathering Worker {worker_id}: SUCCESS")

        return {"success": False, "output": None}

    async def async_stop(self) -> bool:

        # Mark the start time
        self.stop_time = datetime.datetime.now()
        self.duration = (self.stop_time - self.start_time).total_seconds()

        # Tell the cluster to start
        success = await self._async_broadcast_request("post", "/nodes/stop")
        if success:
            self.state.running = False

        # Updating meta just in case of failure
        self._save_meta()

        return success

    async def async_collect(self, unzip: bool = True) -> bool:

        # Then tell them to send the data to the Manager
        success = await self._async_broadcast_request(
            htype="post",
            route="/nodes/collect",
            data={"path": str(self.logdir)},
            timeout=None
            # timeout=max(self.duration * 2, 60),
        )
        await asyncio.sleep(1)

        self.state.collecting = False

        if success:
            try:
                self._save_meta()
                success = self.server.move_transfer_files(self.logdir, unzip)
            except Exception:
                logger.error(traceback.format_exc())
            self.state.collection_status = "PASS"
        else:
            self.state.collection_status = "FAIL"

        logger.info(f"{self}: finished async_collect")

        # Relay information to front-end
        if self.enable_api:
            await self.dashboard_api.broadcast_state_update()

        return success

    async def async_reset(self, keep_workers: bool = True):

        # Destroy Nodes safely
        coros: List[Coroutine] = []
        for worker_id in self.state.workers:
            for node_id in self.state.workers[worker_id].nodes:
                coros.append(self._async_request_node_destruction(worker_id, node_id))

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        # If not keep Workers, then deregister all
        if not keep_workers:
            for worker_id in self.state.workers:
                self._deregister_worker(worker_id)

        # Update variable data
        self.nodes_server_table = {}
        self._deregister_graph()

        return all(results)

    async def async_shutdown(self) -> bool:

        # Only let shutdown happen once
        if self.has_shutdown:
            logger.debug(f"{self}: requested to shutdown twice, skipping.")
            return True
        else:
            self.has_shutdown = True

        logger.debug(f"{self}: shutting down")

        if self.enable_api:
            self.dashboard_api.future_flush()

        if self.enable_zeroconf:
            # Unregister the service and close the zeroconf instance
            self.zeroconf.unregister_service(self.zeroconf_info)
            self.zeroconf.close()

        # If workers are connected, let's notify them that the cluster is
        # shutting down
        if len(self.state.workers) > 0:

            # Send shutdown message
            logger.debug(f"{self}: broadcasting shutdown via /shutdown route")
            try:
                success = await self._async_broadcast_request(
                    "post",
                    "/shutdown",
                    timeout=config.get("manager.timeout.worker-shutdown"),
                    report_exceptions=False,
                )
            except Exception:
                success = False

            if success:
                logger.debug(f"{self}: All workers shutdown: SUCCESS")
            else:
                logger.warning(f"{self}: All workers shutdown: FAILED - forced")

        # Stop the distributed logger
        if self.logs_sink:
            self.logs_sink.shutdown()

        # First, shutdown server
        return await self.server.async_shutdown()

    ####################################################################
    ## Front-facing Sync API
    ####################################################################

    def commit_graph(
        self,
        graph: Graph,
        mapping: Dict[str, List[str]],
        send_packages: Optional[List[Dict[str, Any]]] = None,
    ) -> Future[bool]:
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
            Future[bool]: Future of success in cluster's setup

        """
        return self._exec_coro(self.async_commit(graph, mapping, send_packages))

    def step(self) -> Future[bool]:
        """Cluster step execution for offline operation.

        The ``step`` function is for careful but slow operation of the
        cluster. For online execution, ``start`` and ``stop`` are the
        methods to be used.

        Returns:
            Future[bool]: Future of the success of step function broadcasting

        """
        return self._exec_coro(self._async_broadcast_request("post", "/nodes/step"))

    def gather(self) -> Future[Dict]:
        return self._exec_coro(self.async_gather())

    def start(self) -> Future[bool]:
        """Start the executing of the cluster.

        Before starting, make sure that you have perform the following
        steps:

        - Create ``Nodes``
        - Create ``DAG`` with ``Nodes`` and their edges
        - Connect ``Workers`` (must be before committing ``Graph``)
        - Register, map, and commit ``Graph``

        Returns:
            Future[bool]: Future of the success of starting the cluster

        """
        return self._exec_coro(self.async_start())

    def record(self) -> Future[bool]:
        """Start a recording data collection by the cluster."""
        return self._exec_coro(self.async_record())

    def request_registered_method(
        self, node_id: str, method_name: str, params: Dict[str, Any]
    ) -> Future[Dict[str, Any]]:
        return self._exec_coro(
            self.async_request_registered_method(
                node_id=node_id, method_name=method_name, params=params
            )
        )

    def stop(self) -> Future[bool]:
        """Stop the executiong of the cluster.

        Do not forget that you still need to execute ``shutdown`` to
        properly shutdown processes, threads, and queues.

        Returns:
            Future[bool]: Future of the success of stopping the cluster

        """
        return self._exec_coro(self.async_stop())

    def collect(self, unzip: bool = True) -> Future[bool]:
        """Collect data from the Workers

        First, we wait until all the Nodes have finished save their data.\
        Then, manager request that Nodes' from the Workers.

        Args:
            unzip (bool): Should the .zip archives be extracted.

        Returns:
            Future[bool]: Future of success in collect data from Workers

        """
        return self._exec_coro(self.async_collect(unzip))

    def reset(self, keep_workers: bool = True) -> Future[bool]:
        return self._exec_coro(self.async_reset(keep_workers))

    def shutdown(self, blocking: bool = True) -> Union[bool, Future[bool]]:
        """Proper shutting down ChimeraPy cluster.

        Through this method, the ``Manager`` broadcast to all ``Workers``
        to shutdown, in which they will stop their processes and threads safely.

        """
        future = self._exec_coro(self.async_shutdown())
        if blocking:
            return future.result(timeout=config.get("manager.timeout.worker-shutdown"))
        return future

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
