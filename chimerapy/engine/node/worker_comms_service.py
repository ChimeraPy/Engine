import logging
import pathlib
import tempfile
from typing import Dict, Optional

from aiodistbus import registry

from chimerapy.engine import config

from ..data_protocols import NodeDiagnostics, NodePubTable
from ..networking import Client
from ..networking.enums import NODE_MESSAGE, WORKER_MESSAGE
from ..service import Service
from ..states import NodeState
from .node_config import NodeConfig
from .struct import PreSetupData, RegisteredMethodData


class WorkerCommsService(Service):
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        node_config: NodeConfig,
        worker_logdir: Optional[pathlib.Path] = None,
        worker_config: Optional[Dict] = None,
        logging_level: int = logging.INFO,
        worker_logging_port: int = 5555,
        state: Optional[NodeState] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name=name)

        # Obtaining worker information
        self.host = host
        self.port = port
        self.worker_logging_port = worker_logging_port
        self.worker_config = worker_config
        self.logging_level = logging_level
        self.node_config = node_config

        # Optional
        self.state = state
        self.logger = logger

        if worker_logdir:
            self.worker_logdir = worker_logdir
        else:
            self.worker_logdir = pathlib.Path(tempfile.mktemp())

        # Internal state variables
        self.running: bool = False
        self.client: Optional[Client] = None

    ####################################################################
    ## Helper Functions
    ####################################################################

    @registry.on("pre_setup", PreSetupData, namespace=f"{__name__}.WorkerCommsService")
    def in_node_config(self, presetup_data: PreSetupData):

        # Save parameters
        self.state = presetup_data.state
        self.logger = presetup_data.logger

        if self.worker_config:
            config.update_defaults(self.worker_config)

    def check(self) -> bool:
        if not self.logger:
            raise RuntimeError(f"{self}: logger not set!")
        if not self.state:
            self.logger.error(f"{self}: NodeState not set!")
            return False
        return True

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    @registry.on("setup", namespace=f"{__name__}.WorkerCommsService")
    async def setup(self):
        if not self.check():
            return

        # self.logger.debug(
        #     f"{self}: Prepping the networking component of the Node, connecting to "
        #     f"Worker at {self.host}:{self.port}"
        # )

        # Create client to the Worker
        self.client = Client(
            host=self.host,
            port=self.port,
            id=self.state.id,
            ws_handlers={
                WORKER_MESSAGE.BROADCAST_NODE_SERVER: self.process_node_pub_table,
                WORKER_MESSAGE.REQUEST_STEP: self.async_step,
                WORKER_MESSAGE.REQUEST_COLLECT: self.provide_collect,
                WORKER_MESSAGE.REQUEST_GATHER: self.provide_gather,
                WORKER_MESSAGE.START_NODES: self.start_node,
                WORKER_MESSAGE.RECORD_NODES: self.record_node,
                WORKER_MESSAGE.STOP_NODES: self.stop_node,
                WORKER_MESSAGE.REQUEST_METHOD: self.execute_registered_method,
                WORKER_MESSAGE.DIAGNOSTICS: self.enable_diagnostics,
            },
            parent_logger=self.logger,
        )
        await self.client.async_connect()

        # Send publisher port and host information
        await self.send_state()

    @registry.on("teardown", namespace=f"{__name__}.WorkerCommsService")
    async def teardown(self):

        # Shutdown the client
        if self.client:
            await self.client.async_shutdown()
        # self.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Message Requests
    ####################################################################

    @registry.on(
        "NodeState.changed", NodeState, namespace=f"{__name__}.WorkerCommsService"
    )
    async def send_state(self, state: Optional[NodeState] = None):
        if not self.check():
            return

        # self.logger.debug(f"{self}: Sending NodeState: {self.state.to_dict()}")
        jsonable_state = self.state.to_dict()
        jsonable_state["logdir"] = str(jsonable_state["logdir"])
        if self.client:
            await self.client.async_send(
                signal=NODE_MESSAGE.STATUS, data=jsonable_state
            )

    async def provide_gather(self, msg: Dict):
        if not self.check():
            return

        # self.logger.debug(f"{self}: Sending gather")

        if self.client:
            # self.logger.debug(f"{self}: Sending gather with client")
            await self.entrypoint.emit("gather", self.client)

    @registry.on(
        "report_diagnostics",
        NodeDiagnostics,
        namespace=f"{__name__}.WorkerCommsService",
    )
    async def send_diagnostics(self, diagnostics: NodeDiagnostics):
        if not self.check():
            return

        # self.logger.debug(f"{self}: Sending Diagnostics")

        if self.client:
            data = {"node_id": self.state.id, "diagnostics": diagnostics.to_dict()}
            await self.client.async_send(signal=NODE_MESSAGE.DIAGNOSTICS, data=data)

    ####################################################################
    ## Message Responds
    ####################################################################

    async def process_node_pub_table(self, msg: Dict):
        if not self.check():
            return

        # Pass the information to the Poller Service
        node_pub_table = NodePubTable.from_dict(msg["data"])
        await self.entrypoint.emit("setup_connections", node_pub_table)

    async def start_node(self, msg: Dict = {}):
        if not self.check():
            return
        await self.entrypoint.emit("start")

    async def record_node(self, msg: Dict):
        if not self.check():
            return
        await self.entrypoint.emit("record")

    async def stop_node(self, msg: Dict):
        if not self.check():
            return
        await self.entrypoint.emit("stop")

    async def provide_collect(self, msg: Dict):
        if not self.check():
            return
        await self.entrypoint.emit("collect")

    async def execute_registered_method(self, msg: Dict):
        if not self.check():
            return

        # Check first that the method exists
        method_name, params = (msg["data"]["method_name"], msg["data"]["params"])

        # Send the event
        if self.client:
            event_data = RegisteredMethodData(
                method_name=method_name, params=params, client=self.client
            )
            await self.entrypoint.emit("registered_method", event_data)

    async def async_step(self, msg: Dict):
        if not self.check():
            return
        await self.entrypoint.emit("manual_step")

    async def enable_diagnostics(self, msg: Dict):
        await self.entrypoint.emit("enable_diagnostics", msg["data"]["enable"])
