import logging
from typing import Optional

from chimerapy.engine import _logger
from ..service import Service
from ..states import NodeState
from ..eventbus import EventBus
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

    def setup(self):

        # Creating publisher
        self.publisher = Publisher()
        self.publisher.start()
        self.state.port = self.publisher.port

        # self.node.logger.debug(f"{self} setup")

    def publish(self, data_chunk: DataChunk):
        self.publisher.publish(data_chunk)

    def teardown(self):

        # Shutting down publisher
        if self.publisher:
            self.publisher.shutdown()

        # self.node.logger.debug(f"{self}: shutdown")
