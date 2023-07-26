from typing import Dict, Optional, Literal
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json

from .eventbus import evented
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


@evented
@dataclass_json
@dataclass
class ManagerState:

    # General
    id: str = ""

    # Worker Handler Information
    workers: Dict[str, WorkerState] = field(default_factory=dict)

    # Http Server Information
    ip: str = ""
    port: int = 0

    # Distributed Logging information
    logs_subscription_port: Optional[int] = None
    log_sink_enabled: bool = False
