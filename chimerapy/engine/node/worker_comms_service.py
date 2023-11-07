import logging
from typing import Optional

from aiodistbus import registry

from ..data_protocols import (
    NodePubTable,
)
from ..service import Service
from ..states import NodeState


class WorkerCommsService(Service):
    def __init__(
        self,
        name: str,
        state: Optional[NodeState] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name=name)

        # Optional
        self.state = state
        self.logger = logger

    ####################################################################
    ## Message Requests
    ####################################################################

    @registry.on(
        "NodeState.changed", NodeState, namespace=f"{__name__}.WorkerCommsService"
    )
    async def send_state(self, state: Optional[NodeState] = None):
        await self.entrypoint.emit("node.status", self.state)

    ####################################################################
    ## Message Responds
    ####################################################################

    @registry.on(
        "worker.node_pub_table",
        NodePubTable,
        namespace=f"{__name__}.WorkerCommsService",
    )
    async def process_node_pub_table(self, node_pub_table):
        await self.entrypoint.emit("setup_connections", node_pub_table)
        await self.entrypoint.emit("connected")

    @registry.on("worker.start", namespace=f"{__name__}.WorkerCommsService")
    async def start_node(self):
        await self.entrypoint.emit("start")

    @registry.on("worker.record", namespace=f"{__name__}.WorkerCommsService")
    async def record_node(self):
        await self.entrypoint.emit("record")

    @registry.on("worker.stop", namespace=f"{__name__}.WorkerCommsService")
    async def stop_node(self):
        await self.entrypoint.emit("stop")

    @registry.on("worker.collect", namespace=f"{__name__}.WorkerCommsService")
    async def provide_collect(self):
        await self.entrypoint.emit("collect")
