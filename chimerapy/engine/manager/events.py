from dataclasses import dataclass

from ..states import WorkerState


@dataclass
class StartEvent:
    ...


@dataclass
class WorkerRegisterEvent:  # worker_register
    worker_state: WorkerState


@dataclass
class WorkerDeregisterEvent:  # worker_deregister
    ...
