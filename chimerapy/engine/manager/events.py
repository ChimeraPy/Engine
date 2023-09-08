from dataclasses import dataclass

from ..states import WorkerState, ManagerState


@dataclass
class ManagerStateChangedEvent:
    state: ManagerState


@dataclass
class StartEvent:
    ...


@dataclass
class WorkerRegisterEvent:  # worker_register
    worker_state: WorkerState


@dataclass
class WorkerDeregisterEvent:  # worker_deregister
    worker_state: WorkerState


@dataclass
class RegisterEntityEvent:  # entity_register
    worker_name: str
    worker_id: str


@dataclass
class DeregisterEntityEvent:  # entity_deregister
    worker_id: str


@dataclass
class MoveTransferredFilesEvent:  # move_transferred_files
    unzip: bool
