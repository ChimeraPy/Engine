import pathlib
import threading
import datetime
import logging
from typing import Dict, Optional

from ..networking import Client
from ..states import NodeState
from ..networking.enums import GENERAL_MESSAGE, WORKER_MESSAGE, NODE_MESSAGE
from ..data_protocols import NodePubTable
from ..service import Service
from .node_config import NodeConfig


class WorkerCommsService(Service):
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        worker_logdir: pathlib.Path,
        node_config: NodeConfig,
        logging_level: int = logging.INFO,
        worker_logging_port: int = 5555,
    ):
        super().__init__(name=name)

        # Obtaining worker information
        self.host = host
        self.port = port
        self.worker_logging_port = worker_logging_port
        self.logging_level = logging_level
        self.worker_logdir = worker_logdir
        self.node_config = node_config

        # Internal state variables
        self.running: bool = False
        self.logger = Optional[logging.Logger] = None

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def add_state(self, state: NodeState):
        self.state = state

    def add_logger(self, logger: logging.Logger):
        self.logger = logger

    def setup(self):

        # Events
        self.worker_signal_start = threading.Event()
        self.worker_signal_start.clear()

        # self.logger.debug(
        #     f"{self}: Prepping the networking component of the Node, connecting to \
        #     Worker at {self.host}:{self.port}"
        # )

        # Create client to the Worker
        self.client = Client(
            host=self.host,
            port=self.port,
            id=self.state.id,
            ws_handlers={
                GENERAL_MESSAGE.SHUTDOWN: self.shutdown,
                WORKER_MESSAGE.BROADCAST_NODE_SERVER: self.process_node_pub_table,
                WORKER_MESSAGE.REQUEST_STEP: self.async_step,
                WORKER_MESSAGE.REQUEST_COLLECT: self.provide_collect,
                WORKER_MESSAGE.REQUEST_GATHER: self.provide_gather,
                WORKER_MESSAGE.START_NODES: self.start_node,
                WORKER_MESSAGE.RECORD_NODES: self.record_node,
                WORKER_MESSAGE.STOP_NODES: self.stop_node,
                WORKER_MESSAGE.REQUEST_METHOD: self.execute_registered_method,
            },
            parent_logger=self.logger,
        )
        self.client.connect()

        # Send publisher port and host information
        self.client.send(
            signal=NODE_MESSAGE.STATUS,
            data=self.state.to_dict(),
        )

    def ready(self):

        # Only do so if connected to Worker and its connected
        self.client.send(signal=NODE_MESSAGE.STATUS, data=self.state.to_dict())

    def wait(self):

        # Wait until worker says to start
        while self.running:
            if self.worker_signal_start.wait(timeout=1):
                break

        # Only do so if connected to Worker and its connected
        self.client.send(signal=NODE_MESSAGE.STATUS, data=self.state.to_dict())

    def teardown(self):

        # Inform the worker that the Node has finished its saving of data
        self.client.send(signal=NODE_MESSAGE.STATUS, data=self.state.to_dict())

        # Shutdown the client
        self.client.shutdown()
        # self.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Message Reactivity API
    ####################################################################

    async def process_node_pub_table(self, msg: Dict):

        node_pub_table = NodePubTable.from_json(msg["data"])
        # self.logger.debug(f"{self}: setting up connections: {node_pub_table}")

        # Pass the information to the Poller Service
        if "poller" in self.node.services:
            self.node.services["poller"].setup_connections(node_pub_table)

        self.state.fsm = "CONNECTED"

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )
        # self.logger.debug(f"{self}: Notifying Worker that Node is connected")

    async def start_node(self, msg: Dict):
        self.state.fsm = "PREVIEWING"
        self.worker_signal_start.set()

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )

    async def record_node(self, msg: Dict):
        # self.logger.debug(f"{self}: start")
        self.node.start_time = datetime.datetime.now()
        self.state.fsm = "RECORDING"
        self.worker_signal_start.set()

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )

    async def execute_registered_method(self, msg: Dict):
        # self.logger.debug(f"{self}: execute register method: {msg}")

        # Check first that the method exists
        method_name, params = (msg["data"]["method_name"], msg["data"]["params"])

        if method_name not in self.node.registered_methods:
            results = {
                "node_id": self.node.id,
                "node_state": self.state.to_json(),
                "success": False,
                "output": None,
            }
            self.logger.warning(
                f"{self}: Worker requested execution of registered method that doesn't \
                exists: {method_name}"
            )
        else:
            results = await self.node.services["processor"].execute_registered_method(
                method_name, params
            )
            results.update(
                {"node_id": self.node.id, "node_state": self.state.to_json()}
            )

        await self.client.async_send(signal=NODE_MESSAGE.REPORT_RESULTS, data=results)

    async def async_step(self, msg: Dict):
        # Make the processor take a step
        self.node.services["processor"].forward()

    async def stop_node(self, msg: Dict):
        # Stop by using running variable
        self.state.fsm = "STOPPED"
        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )

    async def provide_gather(self, msg: Dict):

        latest_value = self.node.services["processor"].latest_data_chunk

        await self.client.async_send(
            signal=NODE_MESSAGE.REPORT_GATHER,
            data={
                "state": self.state.to_dict(),
                "latest_value": latest_value.to_json(),
            },
        )

    async def provide_collect(self, msg: Dict):

        # Pass the information to the Record Service
        self.node.services["record"].save()
        self.state.fsm = "SAVED"
        # self.running = False

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )
