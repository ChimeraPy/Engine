from ..service import Service


class NodeService(Service):
    def inject(self, node: "Node"):
        self.node = node
        self.node.services[self.name] = self

    def setup(self):
        ...

    def ready(self):
        ...

    def wait(self):
        ...

    def main(self):
        ...

    def teardown(self):
        ...
