from dataclasses import dataclass

from ..networking.server import FileTransferTable
from ..states import WorkerState


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
class SessionFilesEvent:  # session_files
    file_transfer_records: FileTransferTable


@dataclass
class RecordEvent:
    uuid: str
