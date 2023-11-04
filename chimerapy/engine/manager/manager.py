import os
import pathlib
import uuid
from concurrent.futures import Future
from datetime import datetime
from typing import Any, Coroutine, Dict, List, Literal, Optional, Union

import asyncio_atexit
from aiodistbus import EntryPoint, EventBus, make_evented, registry

# Internal Imports
from chimerapy.engine import _logger, config

from ..data_protocols import CommitData, RegisteredMethodData
from ..graph import Graph
from ..networking.async_loop_thread import AsyncLoopThread
from ..service import Service
from ..states import ManagerState, WorkerState

# Services
from .distributed_logging_service import DistributedLoggingService
from .http_server_service import HttpServerService
from .session_record_service import SessionRecordService
from .worker_handler_service import WorkerHandlerService
from .zeroconf_service import ZeroconfService

logger = _logger.getLogger("chimerapy-engine")


class Manager:
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
        self.publish_logs_via_zmq = publish_logs_via_zmq
        self.enable_api = enable_api
        # self.kwargs = kwargs

        # Creating a container for task futures
        self.task_futures: List[Future] = []
        self.services: List[Service] = []

        # Create log directory to store data
        self.timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        id = str(uuid.uuid4())[:8]
        logdir = pathlib.Path(logdir).resolve() / f"{self.timestamp}-chimerapy-{id}"
        os.makedirs(logdir, exist_ok=True)

        # Create state information container
        self.state = ManagerState(id=id, ip="127.0.0.1", port=port, logdir=logdir)

    async def aserve(self) -> bool:

        # Create eventbus
        self.bus = EventBus()
        self.entrypoint = EntryPoint()
        await self.entrypoint.connect(self.bus)
        self.state = make_evented(self.state, bus=self.bus)

        # Create the services
        self.services.append(
            HttpServerService(
                name="http_server",
                port=self.state.port,
                enable_api=self.enable_api,
                state=self.state,
            )
        )
        self.services.append(
            WorkerHandlerService(name="worker_handler", state=self.state)
        )
        self.services.append(ZeroconfService(name="zeroconf", state=self.state))
        self.services.append(
            SessionRecordService(
                name="session_record",
                state=self.state,
            )
        )
        self.services.append(
            DistributedLoggingService(
                name="distributed_logging",
                publish_logs_via_zmq=self.publish_logs_via_zmq,
                state=self.state,
                # **self.kwargs,
            )
        )
        # Initialize services
        for service in self.services:
            await service.attach(self.bus)

        # Start all services
        await self.entrypoint.emit("start")

        # Logging
        logger.info(f"ChimeraPy: Manager running at {self.host}:{self.port}")

        # Register atexit
        asyncio_atexit.register(self.async_shutdown)

        return True

    def serve(self) -> bool:
        # Create with thread
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Have to run setup before letting the system continue
        return self._exec_coro(self.aserve()).result()

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

    async def _register_graph(self, graph: Graph):
        await self.entrypoint.emit("register_graph", graph)

    async def _deregister_graph(self):
        await self.entrypoint.emit("deregister_graph")

    ####################################################################
    ## EventListeners
    ####################################################################

    @registry.on("registered_method_rep", namespace=f"{__name__}.Manager")
    async def registered_method_rep(self):
        ...

    ####################################################################
    ## Front-facing ASync API
    ####################################################################

    async def async_zeroconf(self, enable: bool = True):
        await self.entrypoint.emit("zeroconf", enable)

    async def async_diagnostics(self, enable: bool = True):
        await self.entrypoint.emit("diagnostics", enable)

    async def async_commit(
        self,
        graph: Graph,
        mapping: Dict[str, List[str]],
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
        send_packages: Optional[List[Dict[str, Any]]] = None,
    ):
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

        """
        commit_data = CommitData(graph, mapping, context, send_packages)
        await self.entrypoint.emit("commit", commit_data)

    async def async_gather(self) -> Dict:
        # TODO
        await self.entrypoint.emit("gather")
        return {}

    async def async_start(self):
        await self.entrypoint.emit("start")

    async def async_record(self):
        await self.entrypoint.emit("record")

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        reg_method_data = RegisteredMethodData(
            node_id=node_id, method_name=method_name, params=params
        )
        await self.entrypoint.emit("request_registered_method", reg_method_data)
        # TODO
        return {}

    async def async_stop(self):
        await self.entrypoint.emit("stop")

    async def async_collect(self):
        await self.entrypoint.emit("collect")

    async def async_reset(self, keep_workers: bool = True):
        await self.entrypoint.emit("reset", keep_workers)

    async def async_shutdown(self) -> bool:

        # Only let shutdown happen once
        if self.has_shutdown:
            # logger.debug(f"{self}: requested to shutdown twice, skipping.")
            return True

        await self.entrypoint.emit("shutdown")
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
