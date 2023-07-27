from .node_service import NodeService
from ..networking import Publisher, DataChunk


class PublisherService(NodeService):

    publisher: Publisher

    def setup(self):

        # Creating publisher
        self.publisher = Publisher()
        self.publisher.start()
        self.node.state.port = self.publisher.port

        # self.node.logger.debug(f"{self} setup")

    def publish(self, data_chunk: DataChunk):
        self.publisher.publish(data_chunk)

    def teardown(self):

        # Shutting down publisher
        if self.publisher:
            self.publisher.shutdown()

        # self.node.logger.debug(f"{self}: shutdown")
