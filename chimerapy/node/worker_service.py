from ..networking import Client
from ..networking.enums import GENERAL_MESSAGE, WORKER_MESSAGE, NODE_MESSAGE
from .node_service import NodeService
from .poller_service import PollerService
from .publisher_service import PublisherService
from .node import Node

import pathlib
import os
import logging
import threading
import datetime
from typing import List, Optional, Dict


class WorkerService(NodeService):
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        worker_logdir: pathlib.Path,
        in_bound: List[str],
        in_bound_by_name: List[str],
        out_bound: List[str],
        follow: Optional[str] = None,
        logging_level: int = logging.INFO,
        worker_logging_port: int = 5555,
    ):
        super().__init__(name=name)

        # Obtaining worker information
        self.host = host
        self.port = port
        self.worker_logging_port = worker_logging_port
        self.logging_level = logging_level
        self.follow = follow
        self.worker_logdir = worker_logdir

        # Storing p2p information
        self.p2p_info = {
            "in_bound": in_bound,
            "in_bound_by_name": in_bound_by_name,
            "out_bound": out_bound,
        }

    def inject(self, node: Node):
        super().inject(node)

        # Creating logdir after given the Node
        self.node.logdir = str(self.worker_logdir / self.node.state.name)
        os.makedirs(self.node.logdir, exist_ok=True)

        # If in-boudn, enable the poller service
        if self.p2p_info["in_bound"]:
            poll_service = PollerService(
                "poller",
                self.p2p_info["in_bound"],
                self.p2p_info["in_bound_by_name"],
                self.follow,
            )
            poll_service.inject(self.node)

        # If out_bound, enable the publisher service
        if self.p2p_info["out_bound"]:
            pub_service = PublisherService("publisher")
            pub_service.inject(self.node)

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def setup(self):

        # Events
        self.worker_signal_start = threading.Event()
        self.worker_signal_start.clear()

        self.node.logger.debug(
            f"{self}: Prepping the networking component of the Node, connecting to \
            Worker at {self.host}:{self.port}"
        )

        # Create client to the Worker
        self.client = Client(
            host=self.host,
            port=self.port,
            id=self.node.state.id,
            ws_handlers={
                GENERAL_MESSAGE.SHUTDOWN: self.shutdown,
                WORKER_MESSAGE.BROADCAST_NODE_SERVER: self.process_node_server_data,
                WORKER_MESSAGE.REQUEST_STEP: self.async_step,
                WORKER_MESSAGE.REQUEST_COLLECT: self.provide_collect,
                WORKER_MESSAGE.REQUEST_GATHER: self.provide_gather,
                WORKER_MESSAGE.START_NODES: self.start_node,
                WORKER_MESSAGE.RECORD_NODES: self.record_node,
                WORKER_MESSAGE.STOP_NODES: self.stop_node,
            },
            parent_logger=self.node.logger,
        )
        self.client.connect()

        # Send publisher port and host information
        self.client.send(
            signal=NODE_MESSAGE.STATUS,
            data=self.node.state.to_dict(),
        )

    def ready(self):

        # Only do so if connected to Worker and its connected
        self.client.send(signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict())

    def wait(self):

        # Wait until worker says to start
        while self.node.running:
            if self.worker_signal_start.wait(timeout=1):
                break

        # Only do so if connected to Worker and its connected
        self.client.send(signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict())

    def teardown(self):

        # Inform the worker that the Node has finished its saving of data
        self.client.send(signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict())

        # Shutdown the client
        self.client.shutdown()

        self.node.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Message Reactivity API
    ####################################################################

    async def process_node_server_data(self, msg: Dict):

        self.node.logger.debug(f"{self}: setting up connections: {msg}")

        # Pass the information to the Poller Service
        if "poller" in self.node.services:
            self.node.services["poller"].setup_connections(msg)

        self.node.state.fsm = "CONNECTED"

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict()
        )
        self.node.logger.debug(f"{self}: Notifying Worker that Node is connected")

    async def start_node(self, msg: Dict):
        self.node.state.fsm = "PREVIEWING"
        self.worker_signal_start.set()

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict()
        )

    async def record_node(self, msg: Dict):
        self.node.logger.debug(f"{self}: start")
        self.node.start_time = datetime.datetime.now()
        self.node.state.fsm = "RECORDING"
        self.worker_signal_start.set()

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict()
        )

    async def async_step(self, msg: Dict):
        # Make the processor take a step
        self.node.services["processor"].forward()

    async def stop_node(self, msg: Dict):
        # Stop by using running variable
        self.node.state.fsm = "STOPPED"
        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict()
        )

    async def provide_gather(self, msg: Dict):

        latest_value = self.node.services["processor"].latest_data_chunk

        await self.client.async_send(
            signal=NODE_MESSAGE.REPORT_GATHER,
            data={
                "state": self.node.state.to_dict(),
                "latest_value": latest_value.to_json(),
            },
        )

    async def provide_collect(self, msg: Dict):

        # Pass the information to the Record Service
        self.node.services["record"].save()
        self.node.state.fsm = "SAVED"
        # self.node.running = False

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.node.state.to_dict()
        )
