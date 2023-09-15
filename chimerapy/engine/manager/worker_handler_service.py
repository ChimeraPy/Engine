import pathlib
import tempfile
import asyncio
import traceback
import json
import pickle
import zipfile
from typing import Dict, Optional, List, Any, Coroutine, Literal, Union

# Third-party Imports
import aiohttp
import networkx as nx

from chimerapy.engine import config
from chimerapy.engine import _logger
from ..utils import async_waiting_for
from ..data_protocols import NodePubTable
from ..node import NodeConfig
from ..networking import Client, DataChunk
from ..service import Service
from ..graph import Graph
from ..exceptions import CommitGraphError
from ..states import WorkerState, ManagerState
from ..eventbus import EventBus, TypedObserver, Event, make_evented
from .events import (
    WorkerRegisterEvent,
    WorkerDeregisterEvent,
    RegisterEntityEvent,
    DeregisterEntityEvent,
    MoveTransferredFilesEvent,
    UpdateSendArchiveEvent,
)

logger = _logger.getLogger("chimerapy-engine")


class WorkerHandlerService(Service):
    def __init__(self, name: str, eventbus: EventBus, state: ManagerState):
        super().__init__(name=name)

        # Parameters
        self.name = name
        self.eventbus = eventbus
        self.state = state

        # Containers
        self.graph: Graph = Graph()
        self.worker_graph_map: Dict = {}
        self.commitable_graph: bool = False
        self.node_pub_table = NodePubTable()
        self.collected_workers: Dict[str, bool] = {}

        # Also create a tempfolder to store any miscellaneous files and folders
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
            "worker_register": TypedObserver(
                "worker_register",
                on_asend=self._register_worker,
                event_data_cls=WorkerRegisterEvent,
                handle_event="unpack",
            ),
            "worker_deregister": TypedObserver(
                "worker_deregister",
                on_asend=self._deregister_worker,
                event_data_cls=WorkerDeregisterEvent,
                handle_event="unpack",
            ),
            "update_send_archive": TypedObserver(
                "update_send_archive",
                on_asend=self.update_send_archive,
                event_data_cls=UpdateSendArchiveEvent,
                handle_event="unpack",
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    async def shutdown(self) -> bool:

        # If workers are connected, let's notify them that the cluster is
        # shutting down
        success = True

        if len(self.state.workers) > 0:

            # Send shutdown message
            try:
                success = await self._broadcast_request(
                    "post",
                    "/shutdown",
                    timeout=config.get("manager.timeout.worker-shutdown"),
                    report_exceptions=False,
                )
            except Exception:
                success = False

            if not success:
                logger.warning(f"{self}: All workers shutdown: FAILED - forced")

        return success

    #####################################################################################
    ## Helper Function
    #####################################################################################

    def _get_worker_ip(self, worker_id: str) -> str:
        worker_info = self.state.workers[worker_id]
        return f"http://{worker_info.ip}:{worker_info.port}"

    async def _register_worker(self, worker_state: WorkerState) -> bool:

        evented_worker_state = make_evented(
            worker_state, event_bus=self.eventbus, event_name="ManagerState.changed"
        )
        self.state.workers[worker_state.id] = evented_worker_state
        logger.debug(
            f"Manager registered <Worker id={worker_state.id}"
            f" name={worker_state.name}> from {worker_state.ip}"
        )

        # Register entity from logging
        await self.eventbus.asend(
            Event(
                "entity_register",
                RegisterEntityEvent(
                    worker_name=worker_state.name, worker_id=worker_state.id
                ),
            )
        )

        return True

    async def _deregister_worker(self, worker_state: WorkerState) -> bool:

        # Deregister entity from logging
        await self.eventbus.asend(
            Event("entity_deregister", DeregisterEntityEvent(worker_id=worker_state.id))
        )

        if worker_state.id in self.state.workers:
            state = self.state.workers[worker_state.id]
            logger.info(
                f"Manager deregistered <Worker id={worker_state.id} name={state.name}> "
                f"from {state.ip}"
            )
            del self.state.workers[worker_state.id]

            return True

        return False

    async def update_send_archive(self, worker_id: str, success: bool):
        self.collected_workers[worker_id] = success

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
                await client.async_send_file(
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

        # logger.debug(f"{self}: Node requested to be created: {worker_id} - {node_id}")

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

        # Send for the creation of the node
        async with aiohttp.ClientSession() as client:
            async with client.post(url=url, data=data) as resp:
                if resp.ok:
                    # logger.debug(
                    #     f"{self}: Node creation ({worker_id}, {node_id}): SUCCESS"
                    # )
                    return True
                else:
                    logger.error(
                        f"{self}: Node creation ({worker_id}, {node_id}): FAILED"
                    )
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

        # Create the data to be send
        data = {
            "worker_id": worker_id,
            "id": node_id,
        }

        # Send for the creation of the node
        async with aiohttp.ClientSession() as client:
            async with client.post(url=url, data=json.dumps(data)) as resp:
                if resp.ok:
                    # logger.debug(
                    #     f"{self}: Node destroy ({worker_id}, {node_id}): SUCCESS"
                    # )
                    return True
                else:

                    logger.error(
                        f"{self}: Node destroy ({worker_id}, {node_id}): FAILED"
                    )
                    return False

    async def _request_node_pub_table(self, worker_id: str) -> bool:
        """Request Workers to provide information about Node's PUBs

        Returns:
            bool: Success of obtaining the node server data

        """
        for i in range(config.get("manager.allowed-failures")):
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    f"{self._get_worker_ip(worker_id)}/nodes/pub_table"
                ) as resp:
                    if resp.ok:
                        # Get JSON
                        data = await resp.json()

                        # And then updating the node server data
                        worker_node_pub_table = NodePubTable.from_json(data)
                        self.node_pub_table.table.update(worker_node_pub_table.table)

                        # logger.debug(
                        #     f"{self}: Requesting Worker's node pub table: SUCCESS"
                        # )
                        return True

                    else:
                        logger.error(
                            f"{self}: Requesting Worker's node pub table: NO \
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
                    f"{self._get_worker_ip(worker_id)}/nodes/pub_table",
                    data=self.node_pub_table.to_json(),
                ) as resp:
                    if resp.ok:

                        # logger.debug(
                        #     f"{self}: receiving Worker's node pub table:" "SUCCESS"
                        # )
                        return True

                    return False

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

        if not self.state.workers:
            return True

        # Create a new session for the moment
        tasks: List[asyncio.Task] = []
        sessions: List[aiohttp.ClientSession] = []
        for worker_data in self.state.workers.values():
            session = aiohttp.ClientSession()
            url = f"http://{worker_data.ip}:{worker_data.port}" + route
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
        # Broadcast request for node server data
        coros: List[Coroutine] = []
        for worker_id in self.worker_graph_map:
            coros.append(self._request_node_pub_table(worker_id))

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

    async def _single_worker_collect(self, worker_id: str) -> bool:

        # Just requesting for the collection to start
        data = {"path": str(self.state.logdir)}
        async with aiohttp.ClientSession() as client:
            async with client.post(
                f"{self._get_worker_ip(worker_id)}/nodes/collect",
                data=json.dumps(data),
            ) as resp:

                if not resp.ok:
                    logger.error(
                        f"{self}: Collection failed, <Worker {worker_id}> "
                        "responded {resp.ok} to collect request"
                    )
                    return False

        # Now we have to wait until worker says they finished transferring
        await async_waiting_for(condition=lambda: worker_id in self.collected_workers)
        success = self.collected_workers[worker_id]
        if not success:
            logger.error(
                f"{self}: Collection failed, <Worker {worker_id}> "
                "never updated on archival completion"
            )

        # Move files to their destination
        try:
            await self.eventbus.asend(
                Event(
                    "move_transferred_files",
                    MoveTransferredFilesEvent(
                        worker_state=self.state.workers[worker_id]
                    ),
                )
            )
        except Exception:
            logger.error(traceback.format_exc())

        return success

    ####################################################################
    ## Front-facing ASync API
    ####################################################################

    async def diagnostics(self, enable: bool = True) -> bool:
        return await self._broadcast_request(
            "post", "/nodes/diagnostics", data={"enable": enable}
        )

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
        await self.eventbus.asend(Event("save_meta"))

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
        await self.eventbus.asend(Event("save_meta"))

        return success

    async def record(self) -> bool:

        # Mark the start time
        await self.eventbus.asend(Event("start_recording"))

        # Tell the cluster to start
        success = await self._broadcast_request("post", "/nodes/record")

        # Updating meta just in case of failure
        await self.eventbus.asend(Event("save_meta"))

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
                    resp_data = await resp.json()
                    return resp_data
                else:
                    logger.debug(
                        f"{self}: Registered Method for Worker {worker_id}: FAILED"
                    )
                    return {"success": False, "output": None}

    async def stop(self) -> bool:

        # Mark the start time
        await self.eventbus.asend(Event("stop_recording"))

        # Tell the cluster to start
        success = await self._broadcast_request("post", "/nodes/stop")

        return success

    async def collect(self) -> bool:

        # Clear
        self.collected_workers.clear()

        # Request all workers
        coros: List[Coroutine] = []
        for worker_id in self.state.workers:
            coros.append(self._single_worker_collect(worker_id))

        try:
            results = await asyncio.gather(*coros)
        except Exception:
            logger.error(traceback.format_exc())
            return False

        await self.eventbus.asend(Event("save_meta"))
        return all(results)

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
            for worker_state in self.state.workers.values():
                await self._deregister_worker(worker_state)

        # Update variable data
        self.node_pub_table = NodePubTable()
        self._deregister_graph()

        return all(results)
