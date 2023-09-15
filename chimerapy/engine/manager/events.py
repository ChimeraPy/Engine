from dataclasses import dataclass

from ..states import WorkerState


@dataclass
class StartEvent:
    ...


@dataclass
class UpdateSendArchiveEvent:  # update_send_archive
    worker_id: str
    success: bool


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
    worker_state: WorkerState
