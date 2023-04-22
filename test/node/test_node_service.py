import chimerapy as cp
from chimerapy.node.node_service import NodeService

logger = cp._logger.getLogger("chimerapy")
cp.debug()


class ExampleService(NodeService):
    def __init__(self, name: str):
        super().__init__(name)
        self.value = False

    def setup(self):
        self.value = True
        logger.debug("HELLO")


def test_service_group(gen_node):

    example_service = ExampleService("example")
    example_service.inject(gen_node)
    gen_node.services.apply("setup", order=["example"])

    assert example_service.value

    gen_node.services.apply("teardown", order=["example"])
