import chimerapy.engine as cpe
from chimerapy.engine.manager.manager_services_group import ManagerServicesGroup
from chimerapy.engine.states import ManagerState

logger = cpe._logger.getLogger("chimerapy-engine")


class DevManagerServicesGroup(ManagerServicesGroup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        state = ManagerState()
        self.inject(state)
        self.apply("start")
        logger.debug(state)
