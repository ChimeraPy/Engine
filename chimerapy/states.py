from typing import List, Dict
from dataclasses import dataclass, field


@dataclass
class NodeState:
    id: str
    name: str = ""
    init: bool = False
    connected: bool = False
    ready: bool = False
    finished: bool = False
    port: int = 0


@dataclass
class WorkerState:
    id: str
    name: str
    port: int = 0
    ip: str = ""
    nodes: Dict[str, NodeState] = field(default_factory=dict)


@dataclass
class ManagerState:
    ip: str = ""
    port: int = 0
    workers: Dict[str, WorkerState] = field(default_factory=dict)
