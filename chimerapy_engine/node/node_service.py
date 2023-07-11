from ..service import Service


class NodeService(Service):
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.node}-{self.__class__.__name__}"

    def inject(self, node):
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
