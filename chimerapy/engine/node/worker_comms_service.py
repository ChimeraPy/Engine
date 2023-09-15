import pathlib
import logging
import tempfile
from typing import Dict, Optional

from ..networking import Client
from ..states import NodeState
from ..networking.enums import GENERAL_MESSAGE, WORKER_MESSAGE, NODE_MESSAGE
from ..data_protocols import NodePubTable, NodeDiagnostics
from ..service import Service
from ..eventbus import EventBus, Event, TypedObserver
from .node_config import NodeConfig
from .events import (
    EnableDiagnosticsEvent,
    ProcessNodePubTableEvent,
    RegisteredMethodEvent,
    GatherEvent,
    DiagnosticsReportEvent,
)


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
        eventbus: Optional[EventBus] = None,
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
        self.eventbus = eventbus

        if worker_logdir:
            self.worker_logdir = worker_logdir
        else:
            self.worker_logdir = pathlib.Path(tempfile.mktemp())

        # Internal state variables
        self.running: bool = False
        self.client: Optional[Client] = None

        # If given the eventbus, add the observers
        if self.eventbus:
            self.add_observers()

    ####################################################################
    ## Helper Functions
    ####################################################################

    def in_node_config(
        self, state: NodeState, logger: logging.Logger, eventbus: EventBus
    ):

        # Save parameters
        self.state = state
        self.logger = logger
        self.eventbus = eventbus

        # Then add observers
        self.add_observers()

    def add_observers(self):
        assert self.state and self.eventbus and self.logger

        # self.logger.debug(f"{self}: adding observers")

        observers: Dict[str, TypedObserver] = {
            "setup": TypedObserver("setup", on_asend=self.setup, handle_event="drop"),
            "NodeState.changed": TypedObserver(
                "NodeState.changed", on_asend=self.send_state, handle_event="drop"
            ),
            "diagnostics_report": TypedObserver(
                "diagnostics_report",
                DiagnosticsReportEvent,
                on_asend=self.send_diagnostics,
                handle_event="unpack",
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
        }
        for ob in observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    async def setup(self):
        assert self.state and self.eventbus and self.logger

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
                GENERAL_MESSAGE.SHUTDOWN: self.shutdown,
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
            thread=self.eventbus.thread,
        )
        await self.client.async_connect()

        # Send publisher port and host information
        await self.send_state()

    async def teardown(self):

        # Shutdown the client
        if self.client:
            await self.client.async_shutdown()
        # self.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Message Requests
    ####################################################################

    async def send_state(self):
        assert self.state and self.eventbus and self.logger

        jsonable_state = self.state.to_dict()
        jsonable_state["logdir"] = str(jsonable_state["logdir"])
        if self.client:
            await self.client.async_send(
                signal=NODE_MESSAGE.STATUS, data=jsonable_state
            )

    async def provide_gather(self, msg: Dict):
        assert self.state and self.eventbus and self.logger

        if self.client:
            event_data = GatherEvent(self.client)
            await self.eventbus.asend(Event("gather", event_data))

    async def send_diagnostics(self, diagnostics: NodeDiagnostics):
        assert self.state and self.eventbus and self.logger

        # self.logger.debug(f"{self}: Sending Diagnostics")

        if self.client:
            data = {"node_id": self.state.id, "diagnostics": diagnostics.to_dict()}
            await self.client.async_send(signal=NODE_MESSAGE.DIAGNOSTICS, data=data)

    ####################################################################
    ## Message Responds
    ####################################################################

    async def process_node_pub_table(self, msg: Dict):
        assert self.state and self.eventbus and self.logger

        node_pub_table = NodePubTable.from_dict(msg["data"])

        # Pass the information to the Poller Service
        event_data = ProcessNodePubTableEvent(node_pub_table)
        await self.eventbus.asend(Event("setup_connections", event_data))

    async def start_node(self, msg: Dict = {}):
        assert self.state and self.eventbus and self.logger
        await self.eventbus.asend(Event("start"))

    async def record_node(self, msg: Dict):
        assert self.state and self.eventbus and self.logger
        await self.eventbus.asend(Event("record"))

    async def stop_node(self, msg: Dict):
        assert self.state and self.eventbus and self.logger
        await self.eventbus.asend(Event("stop"))

    async def provide_collect(self, msg: Dict):
        assert self.state and self.eventbus and self.logger
        await self.eventbus.asend(Event("collect"))

    async def execute_registered_method(self, msg: Dict):
        assert self.state and self.eventbus and self.logger
        # Check first that the method exists
        method_name, params = (msg["data"]["method_name"], msg["data"]["params"])

        # Send the event
        if self.client:
            event_data = RegisteredMethodEvent(
                method_name=method_name, params=params, client=self.client
            )
            await self.eventbus.asend(Event("registered_method", event_data))

    async def async_step(self, msg: Dict):
        assert self.state and self.eventbus and self.logger
        await self.eventbus.asend(Event("manual_step"))

    async def enable_diagnostics(self, msg: Dict):
        assert self.state and self.eventbus and self.logger
        enable = msg["data"]["enable"]

        event_data = EnableDiagnosticsEvent(enable)
        await self.eventbus.asend(Event("enable_diagnostics", event_data))
