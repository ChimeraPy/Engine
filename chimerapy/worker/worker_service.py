from ..service import Service


class WorkerService(Service):
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.worker}-{self.__class__.__name__}"

    def inject(self, worker):
        self.worker = worker
        self.worker.services[self.name] = self

    def start(self):
        ...

    def restart(self):
        ...

    async def shutdown(self):
        ...
