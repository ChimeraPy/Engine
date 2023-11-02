import logging
from typing import Dict, Optional

from aiodistbus import registry

from chimerapy.engine import _logger

from ..networking import DataChunk, Publisher
from ..service import Service
from ..states import NodeState


class PublisherService(Service):

    publisher: Publisher

    def __init__(
        self,
        name: str,
        state: NodeState,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Save information
        self.state = state

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

    @registry.on("setup", namespace=f"{__name__}.PublisherService")
    async def setup(self):

        # Creating publisher
        self.publisher = Publisher()
        self.publisher.start()
        self.state.port = self.publisher.port

    @registry.on("out_step", DataChunk, namespace=f"{__name__}.PublisherService")
    async def publish(self, data_chunk: DataChunk):
        # self.logger.debug(f"{self}: publishing {data_chunk}")
        await self.publisher.publish(data_chunk.to_bytes())

    @registry.on("teardown", namespace=f"{__name__}.PublisherService")
    async def teardown(self):

        # Shutting down publisher
        if self.publisher:
            self.publisher.shutdown()

        # self.logger.debug(f"{self}: shutdown")
