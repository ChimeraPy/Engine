from typing import Union, Dict, Any, Coroutine, Optional, List, Literal, Tuple
import time
import tempfile
import pathlib
import uuid
import shutil
import asyncio
import traceback
from concurrent.futures import Future
from asyncio import Task

from chimerapy import config
from ..networking.async_loop_thread import AsyncLoopThread
from ..logger.zmq_handlers import NodeIDZMQPullListener
from ..states import WorkerState, NodeState
from .. import _logger
from ..service import ServiceGroup
from .manager_client_service import ManagerClientService
from .node_handler_service import NodeHandlerService
from .http_server import HttpServerService


class Worker:

    services: ServiceGroup

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

        # Creating state
        self.state = WorkerState(id=id, name=name, port=port)

        # Creating a container for task futures
        self.task_futures: List[Future] = []

        # Instance variables
        self.has_shutdown: bool = False
        self.shutdown_task: Optional[Task] = None

        # Create temporary data folder
        self.delete_temp = delete_temp
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        parent_logger = _logger.getLogger("chimerapy-worker")
        self.logger = _logger.fork(parent_logger, name, id)

        # Create a log listener to read Node's information
        self.logreceiver = self._start_log_receiver()
        self.logger.debug(f"Log receiver started at port {self.logreceiver.port}")

        # Saving state variables
        self.services = ServiceGroup()

        # Create with thread
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Services to be established
        for s_cls, s_params in {
            NodeHandlerService: {"name": "node_handler"},
            ManagerClientService: {"name": "manager_client"},
            HttpServerService: {"name": "http_server", "thread": self._thread},
        }.items():
            s = s_cls(**s_params)
            s.inject(self)

        # Start all services
        self.services.apply(
            "start", order=["node_handler", "manager_client", "http_server"]
        )

    def __repr__(self):
        return f"<Worker name={self.state.name} id={self.state.id}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Properties
    ####################################################################

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
        return await self.services["manager_client"].async_connect(
            host=host, port=port, method=method, timeout=timeout
        )

    async def async_deregister(self) -> bool:
        return await self.services["manager_client"].async_deregister()

    async def async_create_node(self, node_id: str, msg: Dict) -> bool:
        return await self.services["node_handler"].async_create_node(
            node_id=node_id, msg=msg
        )

    async def async_destroy_node(self, node_id: str) -> bool:
        return await self.services["node_handler"].async_destroy_node(node_id=node_id)

    async def async_start_nodes(self) -> bool:
        return await self.services["node_handler"].async_start_nodes()

    async def async_record_nodes(self) -> bool:
        return await self.services["node_handler"].async_record_nodes()

    async def async_step(self) -> bool:
        return await self.services["node_handler"].async_step()

    async def async_stop_nodes(self) -> bool:
        return await self.services["node_handler"].async_stop_nodes()

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict = {}
    ) -> Dict[str, Any]:
        return await self.services["node_handler"].async_request_registered_method(
            node_id=node_id, method_name=method_name, params=params
        )

    async def async_gather(self) -> Dict:
        return await self.services["node_handler"].async_gather()

    async def async_collect(self) -> bool:
        return await self.services["node_handler"].async_collect()

    async def async_shutdown(self) -> bool:

        # Check if shutdown has been called already
        if self.has_shutdown:
            self.logger.debug(f"{self}: requested to shutdown when already shutdown.")
            return True
        else:
            self.has_shutdown = True

        # Shutdown all services and Wait until all complete
        try:
            results = await asyncio.gather(
                *[s.shutdown() for s in self.services.values()]
            )
        except Exception:
            self.logger.error(traceback.format_exc())
            return False

        # Delete temp folder if requested
        if self.tempfolder.exists() and self.delete_temp:
            shutil.rmtree(self.tempfolder)

        return all(results)

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

    def create_node(self, msg: Dict[str, Any]) -> Future[bool]:
        return self._exec_coro(self.async_create_node(msg["id"], msg))

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

        self.logger.debug(f"{self}: Idle")

        while not self.has_shutdown:
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
        # Check if shutdown coroutine has been created
        if isinstance(self.shutdown_task, Task):
            return self._exec_coro(self._wait_async_shutdown())

        future = self._exec_coro(self.async_shutdown())
        if blocking:
            return future.result(timeout=config.get("manager.timeout.worker-shutdown"))
        return future

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
