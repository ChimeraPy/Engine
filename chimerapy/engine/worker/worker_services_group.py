import typing
from typing import Dict

from chimerapy.engine.service import ServiceGroup
from ..networking.async_loop_thread import AsyncLoopThread

if typing.TYPE_CHECKING:
    from .worker import Worker

from .worker_service import WorkerService
from .http_client_service import HttpClientService
from .node_handler_service import NodeHandlerService
from .http_server_service import HttpServerService


class WorkerServicesGroup(ServiceGroup):  # UserDict

    data: Dict[str, WorkerService]
    http_server: HttpServerService
    http_client: HttpClientService
    node_handler_service: NodeHandlerService

    def __init__(self, worker: "Worker", thread: AsyncLoopThread):
        super().__init__()

        # Saving thread
        self._thread = thread

        # Create the services
        self.http_client = HttpClientService(name="http_client")
        self.http_server = HttpServerService(name="http_server", thread=self._thread)
        self.node_handler = NodeHandlerService(name="node_handler")

        # Save the services into a container for ease of use
        self.data = {
            "http_server": self.http_server,
            "http_client": self.http_client,
            "node_handler": self.node_handler,
        }

        # Inject the services to the Worker
        for service in self.data.values():
            service.inject(worker)
