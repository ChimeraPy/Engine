import asyncio
import pathlib
import shutil
import tempfile
import time
import uuid
from asyncio import Task
from concurrent.futures import Future
from typing import Coroutine, Dict, List, Literal, Optional, Union

import asyncio_atexit
from aiodistbus import DEventBus, EntryPoint, EventBus, make_evented

from chimerapy.engine import _logger, config

from ..data_protocols import ConnectData
from ..logger.zmq_handlers import NodeIDZMQPullListener
from ..networking.async_loop_thread import AsyncLoopThread
from ..service import Service
from ..states import NodeState, WorkerState

# Services
from .http_client_service import HttpClientService
from .http_server_service import HttpServerService
from .node_handler_service import NodeHandlerService


class Worker:
    def __init__(
        self,
        name: str,
        port: int = 0,
        delete_temp: bool = False,
        id: Optional[str] = None,
    ):
        """Create a local Worker.

        To execute ``Nodes`` within the main computer that is also housing
        the ``Manager``, it will require a ``Worker`` as well. Therefore,
        it is common to create a ``Worker`` and a ``Manager`` within the
        same computer.

        To create a worker in another machine, you will have to use the
        following command (in the other machine's terminal):

        >>> cp-worker --ip <manager's IP> --port <manager's port> --name \
            <name> --id <id>

        Args:
            name (str): The name for the ``Worker`` that will be used \
                as reference.
            port (int): The port of the Worker's HTTP server. Select 0 \
                for a random port, mostly when running multiple Worker \
                instances in the same computer.
            delete_temp (bool): After session is over, should the Worker
                delete any of the temporary files.
            id (Optional[str]): Can predefine the ID of the Worker.

        """
        # Saving parameters
        if isinstance(id, str):
            id = id
        else:
            id = str(uuid.uuid4())

        # Create temporary data folder
        self.delete_temp = delete_temp
        tempfolder = pathlib.Path(tempfile.mkdtemp())
        self.state = WorkerState(id=id, name=name, port=port, tempfolder=tempfolder)

        # Creating a container for task futures
        self.services: List[Service] = []
        self.task_futures: List[Future] = []

        # Instance variables
        self._alive: bool = False
        self.shutdown_task: Optional[Task] = None

    async def aserve(self) -> bool:
        """Start the Worker's services.

        This method will start the Worker's services, such as the HTTP
        server and client, and the Node handler. It will also create
        the event bus and the logging artifacts.

        """
        # Create the event bus for the Worker
        self.bus = EventBus()
        self.entrypoint = EntryPoint()
        await self.entrypoint.connect(self.bus)

        # Server: Worker -> Client: Node
        self.dbus = DEventBus()
        await self.dbus.forward(self.bus, event_type=["worker.node.*"])

        # Make the state evented
        self.state = make_evented(self.state, bus=self.bus)

        # Create logging artifacts
        parent_logger = _logger.getLogger("chimerapy-engine-worker")
        self.logger = _logger.fork(parent_logger, self.state.name, self.state.id)

        # Create a log listener to read Node's information
        self.logreceiver = self._start_log_receiver()

        # Create the services
        # self.services.append(
        #     HttpClientService(
        #         name="http_client",
        #         state=self.state,
        #         logger=self.logger,
        #         logreceiver=self.logreceiver,
        #     )
        # )
        # self.services.append(
        #     HttpServerService(
        #         name="http_server",
        #         state=self.state,
        #         logger=self.logger,
        #     )
        # )
        self.services.append(
            NodeHandlerService(
                name="node_handler",
                state=self.state,
                logger=self.logger,
                logreceiver=self.logreceiver,
            )
        )
        for service in self.services:
            await service.attach(self.bus)

        # Start all services
        await self.entrypoint.emit("start")
        self._alive = True

        # Register shutdown
        asyncio_atexit.register(self.shutdown)

        return True

    def serve(self) -> bool:

        # Create with thread
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Have to run setup before letting the system continue
        return self._exec_coro(self.aserve()).result()

    def __repr__(self):
        return f"<Worker name={self.state.name} id={self.state.id}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Properties
    ####################################################################

    @property
    def alive(self) -> bool:
        return self._alive

    @property
    def id(self) -> str:
        return self.state.id

    @property
    def name(self) -> str:
        return self.state.name

    @property
    def nodes(self) -> Dict[str, NodeState]:
        return self.state.nodes

    @property
    def ip(self) -> str:
        return self.state.ip

    @property
    def port(self) -> int:
        return self.state.port

    ####################################################################
    ## Helper Methods
    ####################################################################

    def _exec_coro(self, coro: Coroutine) -> Future:
        # Submitting the coroutine
        future = self._thread.exec(coro)

        # Saving the future for later use
        self.task_futures.append(future)

        return future

    @staticmethod
    def _start_log_receiver() -> NodeIDZMQPullListener:
        log_receiver = _logger.get_node_id_zmq_listener()
        log_receiver.start(register_exit_handlers=True)
        return log_receiver

    async def _wait_async_shutdown(self):

        if isinstance(self.shutdown_task, Task):
            return await self.shutdown_task
        return True

    ####################################################################
    ## Worker ASync Lifecycle API
    ####################################################################

    async def async_connect(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        method: Literal["ip", "zeroconf"] = "ip",
        timeout: Union[int, float] = config.get("worker.timeout.info-request"),
    ):
        """Connect ``Worker`` to ``Manager``.

        This establish server-client connections between ``Worker`` and
        ``Manager``. To ensure that the connections are close correctly,
        either the ``Manager`` or ``Worker`` should shutdown before
        stopping your program to avoid processes and threads that do
        not shutdown.

        Args:
            method (Literal['ip', 'zeroconf']): The approach to connecting to \
                ``Manager``
            host (str): The ``Manager``'s IP address.
            port (int): The ``Manager``'s port number
            timeout (Union[int, float]): Set timeout for the connection.

        Raises:
            TimeoutError: If the connection is not established within the \

        """
        connect_data = ConnectData(method=method, host=host, port=port)
        await asyncio.wait_for(self.entrypoint.emit("connect", connect_data), timeout)

    async def async_deregister(self):
        await self.entrypoint.emit("deregister")

    async def async_shutdown(self) -> bool:

        # Check if shutdown has been called already
        if not self._alive:
            return True
        else:
            self._alive = False

        # Shutdown all services and Wait until all complete
        await self.entrypoint.emit("shutdown")

        # Delete temp folder if requested
        if self.state.tempfolder.exists() and self.delete_temp:
            shutil.rmtree(self.state.tempfolder)

        # self.logger.debug(f"{self}: finished shutdown")

        return True

    ####################################################################
    ## Worker Sync Lifecycle API
    ####################################################################

    def connect(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        method: Literal["ip", "zeroconf"] = "ip",
        timeout: Union[int, float] = 10.0,
        blocking: bool = True,
    ):
        """Connect ``Worker`` to ``Manager``.

        This establish server-client connections between ``Worker`` and
        ``Manager``. To ensure that the connections are close correctly,
        either the ``Manager`` or ``Worker`` should shutdown before
        stopping your program to avoid processes and threads that do
        not shutdown.

        Args:
            host (str): The ``Manager``'s IP address.
            port (int): The ``Manager``'s port number
            timeout (Union[int, float]): Set timeout for the connection.
            blocking (bool): Make the connection call blocking.

        Raises:
            TimeoutError: If the connection is not established within the \
                timeout period.

        """
        future = self._exec_coro(self.async_connect(host, port, method, timeout))

        if blocking:
            return future.result(timeout=timeout)
        return future

    def deregister(self) -> Future[bool]:
        return self._exec_coro(self.async_deregister())

    def idle(self):

        while self._alive:
            time.sleep(2)

    def shutdown(self, blocking: bool = True) -> Union[Future[bool], bool]:
        """Shutdown ``Worker`` safely.

        The ``Worker`` needs to shutdown its server, client and ``Nodes``
        in a safe manner, such as setting flag variables and clearing
        out queues.

        Args:
            msg (Dict): Leave empty, required to work when ``Manager`` sends\
            shutdown message to ``Worker``.

        """
        if not self._alive:
            return True

        self.logger.info(f"{self}: Shutting down")

        # Only execute if thread exists
        if not hasattr(self, "_thread"):
            future: Future[bool] = Future()
            future.set_result(True)
            return future

        # Check if shutdown coroutine has been created
        if isinstance(self.shutdown_task, Task):
            return self._exec_coro(self._wait_async_shutdown())

        future = self._exec_coro(self.async_shutdown())
        if blocking:
            return future.result(timeout=config.get("manager.timeout.worker-shutdown"))
        return future
