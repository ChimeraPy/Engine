import typing

if typing.TYPE_CHECKING:
    from ..states import ManagerState
    from .manager_services_group import ManagerServicesGroup

from ..service import Service


class ManagerService(Service):
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"ManagerService-{self.__class__.__name__}"

    def inject(self, state: "ManagerState", services: "ManagerServicesGroup"):
        # Inject the following attributes
        self.state = state
        self.services = services

    def start(self):
        ...

    def restart(self):
        ...

    async def shutdown(self) -> bool:
        return True
