from typing import Dict, Optional, Literal
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json

from .node.registered_method import RegisteredMethod


@dataclass_json
@dataclass
class NodeState:
    id: str
    name: str = ""
    port: int = 0

    fsm: Literal[
        "NULL",
        "INITIALIZED",
        "CONNECTED",
        "READY",
        "PREVIEWING",
        "RECORDING",
        "STOPPED",
        "SAVED",
        "SHUTDOWN",
    ] = "NULL"

    registered_methods: Dict[str, RegisteredMethod] = field(default_factory=dict)


@dataclass_json
@dataclass
class WorkerState:
    id: str
    name: str
    port: int = 0
    ip: str = ""
    nodes: Dict[str, NodeState] = field(default_factory=dict)


@dataclass_json
@dataclass
class ManagerState:
    id: str = ""
    ip: str = ""
    port: int = 0
    workers: Dict[str, WorkerState] = field(default_factory=dict)

    logs_subscription_port: Optional[int] = None
