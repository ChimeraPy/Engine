import logging
import datetime
from typing import Optional, Dict, List


from chimerapy.engine import _logger
from ..states import NodeState
from ..networking import Subscriber, DataChunk
from ..data_protocols import NodePubTable, NodePubEntry
from ..service import Service
from ..eventbus import EventBus, Event, TypedObserver
from .events import NewInBoundDataEvent, ProcessNodePubTableEvent


class PollerService(Service):
    def __init__(
        self,
        name: str,
        in_bound: List[str],
        in_bound_by_name: List[str],
        state: NodeState,
        eventbus: EventBus,
        follow: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Parameters
        self.in_bound: List[str] = in_bound
        self.in_bound_by_name: List[str] = in_bound_by_name
        self.follow: Optional[str] = follow
        self.state = state
        self.eventbus = eventbus

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

        # Containers
        self.sub: Optional[Subscriber] = None
        self.in_bound_data: Dict[str, DataChunk] = {}

    async def async_init(self):

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
            "setup_connections": TypedObserver(
                "setup_connections",
                ProcessNodePubTableEvent,
                on_asend=self.setup_connections,
                handle_event="unpack",
            ),
        }
        for ob in self.observers.values():
            await self.eventbus.asubscribe(ob)

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    async def teardown(self):

        # Shutting down subscriber
        if self.sub and self.sub.running:
            await self.sub.shutdown()

    ####################################################################
    ## Helper Methods
    ####################################################################

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
            await self.eventbus.asend(
                Event("in_step", NewInBoundDataEvent(self.in_bound_data))
            )
