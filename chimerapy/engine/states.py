import pathlib
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Dict, Literal, Optional

from aiodistbus import global_config
from dataclasses_json import DataClassJsonMixin, cfg

from .data_protocols import NodeDiagnostics
from .node.registered_method import RegisteredMethod


@dataclass
class NodeState(DataClassJsonMixin):
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
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

    # Session logs
    logdir: pathlib.Path = field(
        default_factory=lambda: pathlib.Path(tempfile.mkdtemp())
    )

    # Profiler
    diagnostics: NodeDiagnostics = field(default_factory=NodeDiagnostics)


@dataclass
class WorkerState(DataClassJsonMixin):

    # General
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "default"

    # Node Handler Information
    nodes: Dict[str, NodeState] = field(default_factory=dict)

    # Http Server Information
    ip: str = "0.0.0.0"
    port: int = 0

    # Session logs
    tempfolder: pathlib.Path = field(
        default_factory=lambda: pathlib.Path(tempfile.mkdtemp())
    )


@dataclass
class ManagerState(DataClassJsonMixin):

    # General
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    # Worker Handler Information
    workers: Dict[str, WorkerState] = field(default_factory=dict)

    # Http Server Information
    ip: str = "0.0.0.0"
    port: int = 0

    # Distributed Logging information
    logs_subscription_port: Optional[int] = None
    log_sink_enabled: bool = False

    # Session logs
    logdir: pathlib.Path = pathlib.Path.cwd()


# To reconstruct the state even if evented (essential to make everything to work!)
global_config.set_dtype_mapping(
    "abc.NodeState", f"{NodeState.__module__}.{NodeState.__name__}"
)
global_config.set_dtype_mapping(
    "abc.WorkerState", f"{WorkerState.__module__}.{WorkerState.__name__}"
)
global_config.set_dtype_mapping(
    "abc.ManagerState", f"{ManagerState.__module__}.{ManagerState.__name__}"
)
