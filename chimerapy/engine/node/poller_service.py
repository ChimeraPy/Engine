import datetime
import logging
from typing import Dict, List, Optional

from aiodistbus import EntryPoint, EventBus, registry

from chimerapy.engine import _logger

from ..data_protocols import NodePubEntry, NodePubTable
from ..networking import DataChunk, Subscriber
from ..service import Service
from ..states import NodeState


class PollerService(Service):
    def __init__(
        self,
        name: str,
        in_bound: List[str],
        in_bound_by_name: List[str],
        state: NodeState,
        follow: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Parameters
        self.in_bound: List[str] = in_bound
        self.in_bound_by_name: List[str] = in_bound_by_name
        self.follow: Optional[str] = follow
        self.state = state

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

        # Containers
        self.emit_counter: int = 0
        self.sub: Optional[Subscriber] = None
        self.in_bound_data: Dict[str, DataChunk] = {}

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    @registry.on("teardown", namespace=f"{__name__}.PollerService")
    async def teardown(self):

        # Shutting down subscriber
        if self.sub and self.sub.running:
            await self.sub.shutdown()

    ####################################################################
    ## Helper Methods
    ####################################################################

    @registry.on(
        "setup_connections", NodePubTable, namespace=f"{__name__}.PollerService"
    )
    async def setup_connections(self, node_pub_table: NodePubTable):

        # Create a subscriber
        self.sub = Subscriber()

        # We determine all the out bound nodes
        for i, in_bound_id in enumerate(self.in_bound):

            # Determine the host and port information
            in_bound_entry: NodePubEntry = node_pub_table.table[in_bound_id]

            # Create subscribers to other nodes' publishers
            self.sub.subscribe(
                id=in_bound_id, host=in_bound_entry.ip, port=in_bound_entry.port
            )

        # Start
        self.sub.on_receive(self.update_data)
        await self.sub.start()

    async def update_data(self, datas: Dict[str, bytes]):

        # Default value
        follow_event = False

        # Convert the data to DataChunk
        for k, d in datas.items():

            # Reconstruct the DataChunk and marked when it was received
            data_chunk = DataChunk.from_bytes(d)
            meta = data_chunk.get("meta")
            meta["value"]["received"] = datetime.datetime.now()
            data_chunk.update("meta", meta)

            # Update the latest value
            nickname = self.in_bound_by_name[self.in_bound.index(k)]
            self.in_bound_data[nickname] = data_chunk

            # Update flag if new values are coming from the node that is
            # being followed
            if self.follow == k:
                follow_event = True

        # If update on the follow and all inputs available, then use the inputs
        if follow_event and len(self.in_bound_data) == len(self.in_bound):
            await self.entrypoint.emit("in_step", self.in_bound_data)
            self.emit_counter += 1
