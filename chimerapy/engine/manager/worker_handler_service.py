from typing import Dict, Optional, List, Any, Coroutine, Literal, Union
import pathlib
import tempfile
import asyncio
import traceback
import json
import pickle
import zipfile

# Third-party Imports
import aiohttp
import networkx as nx

from chimerapy.engine import config
from ..node import NodeConfig
from chimerapy.engine.graph import Graph
from ..networking import Client, DataChunk
from chimerapy.engine.exceptions import CommitGraphError
from chimerapy.engine.states import WorkerState, NodeState
from .manager_service import ManagerService
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy")


class WorkerHandlerService(ManagerService):
    def __init__(self, name: str):
        super().__init__(name=name)

        self.name = name
        self.graph: Graph = Graph()
        self.worker_graph_map: Dict = {}
        self.commitable_graph: bool = False
        self.nodes_server_table: Dict = {}

        # Also create a tempfolder to store any miscellaneous files and folders
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

    async def shutdown(self) -> bool:

        # If workers are connected, let's notify them that the cluster is
        # shutting down
        success = True

        if len(self.state.workers) > 0:

            # Send shutdown message
            logger.debug(f"{self}: broadcasting shutdown via /shutdown route")
            try:
                success = await self._broadcast_request(
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

        return success

    #####################################################################################
    ## Helper Function
    #####################################################################################

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

        # Register entity from logging
        self.services.distributed_logging.register_entity(
            worker_state.name, worker_state.id
        )

        return True

    def _deregister_worker(self, worker_id: str) -> bool:

        # Deregister entity from logging
        self.services.distributed_logging.deregister_entity(worker_id)

        if worker_id in self.state.workers:
            state = self.state.workers[worker_id]
            logger.info(
                f"Manager deregistered <Worker id={worker_id} name={state.name}> \
                from {state.ip}"
            )
            del self.state.workers[worker_id]

            return True

        return False

    def _register_graph(self, graph: Graph):
        """Verifying that a Graph is valid, that is a DAG.

        In ChimeraPy-Engine, cycle are not allowed, this is to avoid a deadlock.
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

        The mapping, a dictionary, informs ChimeraPy-Engine which ``Worker`` is
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

    def _node_to_worker_lookup(self, node_id: str) -> Optional[str]:

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
                zip_pkg.writepy(package_path)  # type: ignore[attr-defined]

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
                    async with client.post(  # type: ignore[attr-defined]
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

    async def _request_node_creation(
        self,
        worker_id: str,
        node_id: str,
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
    ) -> bool:
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
            NodeConfig(
                node=self.graph.G.nodes[node_id]["object"],
                in_bound=in_bound,
                in_bound_by_name=in_bound_by_name,
                out_bound=list(self.graph.G.successors(node_id)),
                follow=self.graph.G.nodes[node_id]["follow"],
                context=context,
            )
        )

        # Construct the url
        url = f"{self._get_worker_ip(worker_id)}/nodes/create"
        logger.debug(f"{self}: Sending request to {url}")

        # Send for the creation of the node
        async with aiohttp.ClientSession() as client:
            async with client.post(url=url, data=data) as resp:
                if resp.ok:
                    json_data: Dict[str, Any] = await resp.json()

                    if not json_data["success"]:
                        logger.error(
                            f"{self}: Node creation ({worker_id}, {node_id}): FAILED"
                        )
                        return False

                    logger.debug(
                        f"{self}: Node creation ({worker_id}, {node_id}): SUCCESS"
                    )
                    self.state.workers[worker_id].nodes[node_id] = NodeState.from_dict(
                        json_data["node_state"]
                    )
                    logger.debug(f"{self}: WorkerState: {self.state.workers}")

                    # Relay information to front-end
                    await self.services.http_server._broadcast_network_status_update()

                    return True

        logger.error(f"{self}: Worker {worker_id} didn't respond!")

        return False

    async def _request_node_destruction(self, worker_id: str, node_id: str) -> bool:
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

                    if not data["success"]:
                        logger.error(
                            f"{self}: Node destroy ({worker_id}, {node_id}): FAILED"
                        )
                        return False

                    logger.debug(
                        f"{self}: Node destroy ({worker_id}, {node_id}): SUCCESS"
                    )
                    self.state.workers[worker_id] = WorkerState.from_dict(
                        data["worker_state"]
                    )
                    logger.debug(f"{self}: WorkerState: {self.state.workers}")

                    # Relay information to front-end
                    await self.services.http_server._broadcast_network_status_update()

                    return True

        logger.error(f"{self}: Worker {worker_id} didn't respond!")

        return False

    async def _request_node_server_data(self, worker_id: str) -> bool:
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

    async def _request_connection_creation(self, worker_id: str) -> bool:
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
                    if not resp.ok:
                        return False

                    # Get JSON
                    data = await resp.json()

                    if not data["success"]:
                        return False

                    self.state.workers[worker_id] = WorkerState.from_dict(
                        data["worker_state"]
                    )
                    logger.debug(f"{self}: WorkerState: {self.state.workers}")

                    logger.debug(
                        f"{self}: receiving Worker's node server request: \
                        SUCCESS"
                    )
                    # Relay information to front-end
                    await self.services.http_server._broadcast_network_status_update()

                    return True

        return False

    async def _broadcast_request(
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

    async def _create_p2p_network(
        self, context: Literal["multiprocessing", "threading"] = "multiprocessing"
    ) -> bool:
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
                coro = self._request_node_creation(worker_id, node_id, context=context)
                coros.append(coro)

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        return all(results)

    async def _setup_p2p_connections(self) -> bool:
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
            coros.append(self._request_node_server_data(worker_id))

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        if not all(results):
            return False

        # Distribute the entire graph's information to all the Workers
        coros2: List[Coroutine] = []
        for worker_id in self.worker_graph_map:
            coros2.append(self._request_connection_creation(worker_id))

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros2)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        return all(results)

    ####################################################################
    ## Front-facing ASync API
    ####################################################################

    async def commit(
        self,
        graph: Graph,
        mapping: Dict[str, List[str]],
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
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
        self.services.session_record._save_meta()

        # Then send requested packages
        success = True
        if send_packages:
            success = await self._distribute_packages(send_packages)

        # If package are sent correctly, try to create network
        # Start with the nodes and then the connections
        if (
            success
            and await self._create_p2p_network(context=context)
            and await self._setup_p2p_connections()
        ):
            return True

        # Relay information to front-end
        await self.services.http_server._broadcast_network_status_update()

        return False

    async def gather(self) -> Dict:
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

    async def start_workers(self) -> bool:

        # Tell the cluster to start
        success = await self._broadcast_request("post", "/nodes/start")

        # Updating meta just in case of failure
        self.services.session_record._save_meta()

        # Relay information to front-end
        await self.services.http_server._broadcast_network_status_update()

        return success

    async def record(self) -> bool:

        # Mark the start time
        self.services.session_record.start_recording()

        # Tell the cluster to start
        success = await self._broadcast_request("post", "/nodes/record")

        # Updating meta just in case of failure
        self.services.session_record._save_meta()

        # Relay information to front-end
        await self.services.http_server._broadcast_network_status_update()

        return success

    async def request_registered_method(
        self, node_id: str, method_name: str, params: Dict[str, Any] = {}
    ) -> Dict[str, Any]:

        # First, identify which worker has the node
        worker_id = self._node_to_worker_lookup(node_id)

        if not isinstance(worker_id, str):
            return {"success": False, "output": None}

        data = {
            "node_id": str(node_id),
            "method_name": str(method_name),
            "params": dict(params),
        }

        async with aiohttp.ClientSession() as client:
            async with client.post(
                f"{self._get_worker_ip(worker_id)}/nodes/registered_methods",
                data=json.dumps(data),
            ) as resp:

                if resp.ok:
                    logger.debug(
                        f"{self}: Registered Method for Worker {worker_id}: SUCCESS"
                    )
                    resp_data = await resp.json()
                    return resp_data
                else:
                    logger.debug(
                        f"{self}: Registered Method for Worker {worker_id}: FAILED"
                    )
                    return {"success": False, "output": None}

    async def stop(self) -> bool:

        # Mark the start time
        self.services.session_record.stop_recording()

        # Tell the cluster to start
        success = await self._broadcast_request("post", "/nodes/stop")

        # Updating meta just in case of failure
        self.services.session_record._save_meta()

        # Relay information to front-end
        await self.services.http_server._broadcast_network_status_update()

        return success

    async def collect(self, unzip: bool = True) -> bool:

        # Then tell them to send the data to the Manager
        success = await self._broadcast_request(
            htype="post",
            route="/nodes/collect",
            data={"path": str(self.services.session_record.logdir)},
            timeout=None,
        )
        await asyncio.sleep(1)

        if success:
            try:
                self.services.session_record._save_meta()
                success = self.services.http_server._server.move_transfer_files(
                    self.services.session_record.logdir, unzip
                )
            except Exception:
                logger.error(traceback.format_exc())

        logger.info(f"{self}: finished collect")

        # Relay information to front-end
        await self.services.http_server._broadcast_network_status_update()

        return success

    async def reset(self, keep_workers: bool = True):

        # Destroy Nodes safely
        coros: List[Coroutine] = []
        for worker_id in self.state.workers:
            for node_id in self.state.workers[worker_id].nodes:
                coros.append(self._request_node_destruction(worker_id, node_id))

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

        # Relay information to front-end
        await self.services.http_server._broadcast_network_status_update()

        return all(results)
