import logging
from typing import Optional, Dict

from chimerapy.engine import _logger
from ..service import Service
from ..states import NodeState
from ..eventbus import EventBus, TypedObserver
from ..networking import Publisher, DataChunk


class PublisherService(Service):

    publisher: Publisher

    def __init__(
        self,
        name: str,
        state: NodeState,
        eventbus: EventBus,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Save information
        self.state = state
        self.eventbus = eventbus

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

        # Add observer
        self.observers: Dict[str, TypedObserver] = {
            "setup": TypedObserver("setup", on_asend=self.setup, handle_event="drop"),
            "out_step": TypedObserver(
                "out_step", on_asend=self.publish, handle_event="unpack"
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    def setup(self):

        # Creating publisher
        self.publisher = Publisher()
        self.publisher.start()
        self.state.port = self.publisher.port

    def publish(self, data_chunk: DataChunk):
        # self.logger.debug(f"{self}: publishing {data_chunk}")
        self.publisher.publish(data_chunk)

    def teardown(self):

        # Shutting down publisher
        if self.publisher:
            self.publisher.shutdown()

        # self.logger.debug(f"{self}: shutdown")
