from chimerapy.engine.service import Service


class WorkerService(Service):
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.worker}-{self.__class__.__name__}"

    def inject(self, worker):
        self.worker = worker

    def start(self):
        ...

    def restart(self):
        ...

    async def shutdown(self):
        ...
