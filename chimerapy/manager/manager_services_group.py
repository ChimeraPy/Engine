import typing
from typing import Dict
import pathlib
from typing import Union, Optional

from ..states import ManagerState
from ..service import ServiceGroup
from ..networking.async_loop_thread import AsyncLoopThread

if typing.TYPE_CHECKING:
    pass

from .manager_service import ManagerService
from .http_server_service import HttpServerService
from .worker_handler_service import WorkerHandlerService
from .zeroconf_service import ZeroconfService
from .session_record_service import SessionRecordService
from .distributed_logging_service import DistributedLoggingService


class ManagerServicesGroup(ServiceGroup):  # UserDict

    data: Dict[str, ManagerService]
    http_server: HttpServerService
    worker_handler: WorkerHandlerService
    zeroconf: ZeroconfService
    session_record: SessionRecordService
    distributed_logging: DistributedLoggingService

    def __init__(
        self,
        logdir: Union[str, pathlib.Path],
        port: int = 0,
        publish_logs_via_zmq: bool = True,
        enable_api: bool = True,
        thread: Optional[AsyncLoopThread] = None,
        **kwargs
    ):
        super().__init__()

        # Saving thread and manager
        if thread:
            self._thread = thread
        else:
            self._thread = AsyncLoopThread()

        # Create the services
        self.http_server = HttpServerService(
            name="http_server",
            port=port,
            enable_api=enable_api,
            thread=self._thread,
        )
        self.worker_handler = WorkerHandlerService(name="worker_handler")
        self.zeroconf = ZeroconfService(name="zeroconf")
        self.session_record = SessionRecordService(name="session_record", logdir=logdir)
        self.distributed_logging = DistributedLoggingService(
            name="distributed_logging",
            publish_logs_via_zmq=publish_logs_via_zmq,
            **kwargs
        )

        # Save the services into a container for ease of use
        self.data = {
            "http_server": self.http_server,
            "worker_handler": self.worker_handler,
            "zeroconf": self.zeroconf,
            "session_record": self.session_record,
        }

    def inject(self, state: "ManagerState"):

        # Inject the services to the Worker
        for service in self.data.values():
            service.inject(state, self)
