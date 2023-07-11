import chimerapy_engine as cpe
from chimerapy_engine.manager.manager_services_group import ManagerServicesGroup
from chimerapy_engine.states import ManagerState

logger = cpe._logger.getLogger("chimerapy")


class DevManagerServicesGroup(ManagerServicesGroup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        state = ManagerState()
        self.inject(state)
        self.apply("start")
        logger.debug(state)
