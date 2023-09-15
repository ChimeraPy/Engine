import pathlib
import atexit
import os
import uuid
from datetime import datetime
from concurrent.futures import Future
from typing import Dict, Optional, List, Union, Any, Literal, Coroutine

# Internal Imports
from chimerapy.engine import config
from chimerapy.engine import _logger
from ..networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.states import ManagerState, WorkerState
from chimerapy.engine.graph import Graph

# Eventbus
from ..eventbus import EventBus, Event, make_evented

# from .events import StartEvent

# Services
from .http_server_service import HttpServerService
from .worker_handler_service import WorkerHandlerService
from .zeroconf_service import ZeroconfService
from .session_record_service import SessionRecordService
from .distributed_logging_service import DistributedLoggingService

logger = _logger.getLogger("chimerapy-engine")


class Manager:

    http_server: HttpServerService
    worker_handler: WorkerHandlerService
    zeroconf_service: ZeroconfService
    session_record: SessionRecordService
    distributed_logging: DistributedLoggingService

    def __init__(
        self,
        logdir: Union[pathlib.Path, str],
        port: int = 9000,
        publish_logs_via_zmq: bool = False,
        enable_api: bool = True,
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

            **kwargs: Additional keyword arguments.
                Currently, this is used to configure the ZMQ log handler.
        """
        # Saving input parameters
        self.has_shutdown = False

        # Creating a container for task futures
        self.task_futures: List[Future] = []

        # Create with thread
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Create eventbus
        self.eventbus = EventBus(thread=self._thread)

        # Create log directory to store data
        self.timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        id = str(uuid.uuid4())[:8]
        logdir = pathlib.Path(logdir).resolve() / f"{self.timestamp}-chimerapy-{id}"
        os.makedirs(logdir, exist_ok=True)

        # Create state information container
        self.state = make_evented(
            ManagerState(id=id, ip="127.0.0.1", port=port, logdir=logdir),
            event_bus=self.eventbus,
        )

        # Create the services
        self.http_server = HttpServerService(
            name="http_server",
            port=port,
            enable_api=enable_api,
            thread=self._thread,
            eventbus=self.eventbus,
            state=self.state,
        )
        self.worker_handler = WorkerHandlerService(
            name="worker_handler", eventbus=self.eventbus, state=self.state
        )
        self.zeroconf_service = ZeroconfService(
            name="zeroconf", eventbus=self.eventbus, state=self.state
        )
        self.session_record = SessionRecordService(
            name="session_record",
            eventbus=self.eventbus,
            state=self.state,
        )
        self.distributed_logging = DistributedLoggingService(
            name="distributed_logging",
            publish_logs_via_zmq=publish_logs_via_zmq,
            eventbus=self.eventbus,
            state=self.state,
            **kwargs,
        )

        # Start all services
        self.eventbus.send(Event("start")).result(timeout=10)

        # Logging
        logger.info(f"ChimeraPy: Manager running at {self.host}:{self.port}")

        # Register atexit
        atexit.register(self.shutdown)

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

    @property
    def logdir(self) -> pathlib.Path:
        return self.state.logdir

    ####################################################################
    ## Utils Methods
    ####################################################################

    def _exec_coro(self, coro: Coroutine) -> Future:
        # Submitting the coroutine
        future = self._thread.exec(coro)

        # Saving the future for later use
        self.task_futures.append(future)

        return future

    ####################################################################
    ## Async Networking
    ####################################################################

    async def _async_request_node_creation(
        self,
        worker_id: str,
        node_id: str,
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
    ) -> bool:
        return await self.worker_handler._request_node_creation(
            worker_id, node_id, context=context
        )

    async def _async_request_node_destruction(
        self, worker_id: str, node_id: str
    ) -> bool:
        return await self.worker_handler._request_node_destruction(worker_id, node_id)

    async def _async_request_node_pub_table(self, worker_id: str) -> bool:
        return await self.worker_handler._request_node_pub_table(worker_id)

    async def _async_request_connection_creation(self, worker_id: str) -> bool:
        return await self.worker_handler._request_connection_creation(worker_id)

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
        return await self.worker_handler._broadcast_request(
            htype, route, data, timeout, report_exceptions
        )

    ####################################################################
    ## Sync Networking
    ####################################################################

    def _register_graph(self, graph: Graph):
        self.worker_handler._register_graph(graph)

    def _deregister_graph(self):
        self.worker_handler._deregister_graph()

    def _request_node_creation(
        self,
        worker_id: str,
        node_id: str,
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
    ) -> Future[bool]:
        return self._exec_coro(
            self._async_request_node_creation(worker_id, node_id, context=context)
        )

    def _request_node_destruction(self, worker_id: str, node_id: str) -> Future[bool]:
        return self._exec_coro(self._async_request_node_destruction(worker_id, node_id))

    def _request_node_pub_table(self, worker_id: str) -> Future[bool]:
        return self._exec_coro(self._async_request_node_pub_table(worker_id))

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

    async def async_zeroconf(self, enable: bool = True) -> bool:
        if enable:
            return await self.zeroconf_service.enable()
        else:
            return await self.zeroconf_service.disable()

    async def async_diagnostics(self, enable: bool = True) -> bool:
        return await self.worker_handler.diagnostics(enable)

    async def async_commit(
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
        return await self.worker_handler.commit(
            graph, mapping, context=context, send_packages=send_packages
        )

    async def async_gather(self) -> Dict:
        return await self.worker_handler.gather()

    async def async_start(self) -> bool:
        return await self.worker_handler.start_workers()

    async def async_record(self) -> bool:
        return await self.worker_handler.record()

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        return await self.worker_handler.request_registered_method(
            node_id, method_name, params
        )

    async def async_stop(self) -> bool:
        return await self.worker_handler.stop()

    async def async_collect(self) -> bool:
        return await self.worker_handler.collect()

    async def async_reset(self, keep_workers: bool = True):
        return await self.worker_handler.reset(keep_workers)

    async def async_shutdown(self) -> bool:

        # Only let shutdown happen once
        if self.has_shutdown:
            # logger.debug(f"{self}: requested to shutdown twice, skipping.")
            return True

        await self.eventbus.asend(Event("shutdown"))
        self.has_shutdown = True

        return True

    ####################################################################
    ## Front-facing Sync API
    ####################################################################

    def zeroconf(self, enable: bool = True, timeout: Union[int, float] = 5) -> bool:
        return self._exec_coro(self.async_zeroconf(enable)).result(timeout)

    def diagnostics(self, enable: bool = True, timeout: Union[int, float] = 5) -> bool:
        return self._exec_coro(self.async_diagnostics(enable)).result(timeout)

    def commit_graph(
        self,
        graph: Graph,
        mapping: Dict[str, List[str]],
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
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
        return self._exec_coro(
            self.async_commit(
                graph, mapping, context=context, send_packages=send_packages
            )
        )

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
        self, node_id: str, method_name: str, params: Dict[str, Any] = {}
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

    def collect(self) -> Future[bool]:
        """Collect data from the Workers

        First, we wait until all the Nodes have finished save their data.\
        Then, manager request that Nodes' from the Workers.

        Returns:
            Future[bool]: Future of success in collect data from Workers

        """
        return self._exec_coro(self.async_collect())

    def reset(
        self, keep_workers: bool = True, blocking: bool = True
    ) -> Union[bool, Future[bool]]:
        future = self._exec_coro(self.async_reset(keep_workers))

        if blocking:
            return future.result(timeout=config.get("manager.timeout.reset"))

        return future

    def shutdown(self, blocking: bool = True) -> Union[bool, Future[bool]]:
        """Proper shutting down ChimeraPy-Engine cluster.

        Through this method, the ``Manager`` broadcast to all ``Workers``
        to shutdown, in which they will stop their processes and threads safely.

        """
        future = self._exec_coro(self.async_shutdown())
        if blocking:
            return future.result(timeout=config.get("manager.timeout.worker-shutdown"))
        return future
