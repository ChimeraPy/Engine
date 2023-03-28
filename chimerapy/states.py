from typing import List, Dict, Optional, Literal
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class RegisteredMethod:
    name: str
    style: str = "concurrent"  # Literal['concurrent', 'blocking', 'reset']
    params: Dict[str, str] = field(default_factory=dict)


@dataclass_json
@dataclass
class NodeState:
    id: str
    name: str = ""
    init: bool = False
    connected: bool = False
    ready: bool = False
    finished: bool = False
    port: int = 0
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
    running: bool = False
    collecting: bool = False
    collection_status: Optional[Literal["PASS", "FAIL"]] = None
