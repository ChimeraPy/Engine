from typing import Union, Dict, Any, Coroutine, Optional, List, Literal, Tuple
import time
import tempfile
import pathlib
import uuid
import shutil
import atexit
from concurrent.futures import Future
from asyncio import Task

from chimerapy.engine import config
from chimerapy.engine import _logger
from ..networking.async_loop_thread import AsyncLoopThread
from ..logger.zmq_handlers import NodeIDZMQPullListener
from ..states import WorkerState, NodeState
from ..node import NodeConfig
from ..eventbus import EventBus, Event, make_evented
from .http_server_service import HttpServerService
from .http_client_service import HttpClientService
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

        # Creating a container for task futures
        self.task_futures: List[Future] = []

        # Instance variables
        self._alive: bool = False
        self.shutdown_task: Optional[Task] = None

        # Create with thread
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Create the event bus for the Worker
        self.eventbus = EventBus(thread=self._thread)

        # Creating state
        self.state = make_evented(
            WorkerState(id=id, name=name, port=port, tempfolder=tempfolder),
            event_bus=self.eventbus,
        )
        # Create logging artifacts
        parent_logger = _logger.getLogger("chimerapy-engine-worker")
        self.logger = _logger.fork(parent_logger, name, id)

        # Create a log listener to read Node's information
        self.logreceiver = self._start_log_receiver()

        # Create the services
        self.http_client = HttpClientService(
            name="http_client",
            state=self.state,
            eventbus=self.eventbus,
            logger=self.logger,
            logreceiver=self.logreceiver,
        )
        self.http_server = HttpServerService(
            name="http_server",
            state=self.state,
            thread=self._thread,
            eventbus=self.eventbus,
            logger=self.logger,
        )
        self.node_handler = NodeHandlerService(
            name="node_handler",
            state=self.state,
            eventbus=self.eventbus,
            logger=self.logger,
            logreceiver=self.logreceiver,
        )

        # Start all services
        self.eventbus.send(Event("start")).result(timeout=10)
        self._alive = True

        # Register shutdown
        atexit.register(self.shutdown)

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
        method: Optional[Literal["ip", "zeroconf"]] = "ip",
        timeout: Union[int, float] = config.get("worker.timeout.info-request"),
    ) -> bool:
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

        Returns:
            bool: Success in connecting to the Manager

        """
        return await self.http_client.async_connect(
            host=host, port=port, method=method, timeout=timeout
        )

    async def async_deregister(self) -> bool:
        return await self.http_client.async_deregister()

    async def async_create_node(self, node_config: Union[NodeConfig, Dict]) -> bool:
        return await self.node_handler.async_create_node(node_config)

    async def async_destroy_node(self, node_id: str) -> bool:
        return await self.node_handler.async_destroy_node(node_id=node_id)

    async def async_start_nodes(self) -> bool:
        return await self.node_handler.async_start_nodes()

    async def async_record_nodes(self) -> bool:
        return await self.node_handler.async_record_nodes()

    async def async_step(self) -> bool:
        return await self.node_handler.async_step()

    async def async_stop_nodes(self) -> bool:
        return await self.node_handler.async_stop_nodes()

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict = {}
    ) -> Dict[str, Any]:
        return await self.node_handler.async_request_registered_method(
            node_id=node_id, method_name=method_name, params=params
        )

    async def async_gather(self) -> Dict:
        return await self.node_handler.async_gather()

    async def async_collect(self) -> bool:
        return await self.node_handler.async_collect()

    async def async_shutdown(self) -> bool:

        # Check if shutdown has been called already
        if not self._alive:
            return True
        else:
            self._alive = False

        # Shutdown all services and Wait until all complete
        await self.eventbus.asend(Event("shutdown"))

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
        method: Optional[Literal["ip", "zeroconf"]] = "ip",
        timeout: Union[int, float] = 10.0,
        blocking: bool = True,
    ) -> Union[bool, Future[bool]]:
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

        Returns:
            Future[bool]: Success in connecting to the Manager

        """
        future = self._exec_coro(self.async_connect(host, port, method, timeout))

        if blocking:
            return future.result(timeout=timeout)
        return future

    def deregister(self) -> Future[bool]:
        return self._exec_coro(self.async_deregister())

    def create_node(self, node_config: NodeConfig) -> Future[bool]:
        return self._exec_coro(self.async_create_node(node_config))

    def destroy_node(self, node_id: str) -> Future[bool]:
        return self._exec_coro(self.async_destroy_node(node_id))

    def step(self) -> Future[bool]:
        return self._exec_coro(self.async_step())

    def start_nodes(self) -> Future[bool]:
        return self._exec_coro(self.async_start_nodes())

    def record_nodes(self) -> Future[bool]:
        return self._exec_coro(self.async_record_nodes())

    def request_registered_method(
        self,
        node_id: str,
        method_name: str,
        params: Dict = {},
    ) -> Future[Tuple[bool, Any]]:
        return self._exec_coro(
            self.async_request_registered_method(
                node_id=node_id, method_name=method_name, params=params
            )
        )

    def stop_nodes(self) -> Future[bool]:
        return self._exec_coro(self.async_stop_nodes())

    def gather(self) -> Future[Dict]:
        return self._exec_coro(self.async_gather())

    def collect(self) -> Future[bool]:
        return self._exec_coro(self.async_collect())

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
