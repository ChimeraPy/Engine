import chimerapy as cp
from chimerapy.manager.manager_services_group import ManagerServicesGroup
from chimerapy.states import ManagerState

logger = cp._logger.getLogger("chimerapy")


class DevManagerServicesGroup(ManagerServicesGroup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        state = ManagerState()
        self.inject(state)
        self.apply("start")
        logger.debug(state)
