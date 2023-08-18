import pathlib
import uuid
import tempfile
from typing import Dict, Optional, Literal
from dataclasses import dataclass, field
from dataclasses_json import DataClassJsonMixin, cfg

from .node.registered_method import RegisteredMethod
from .data_protocols import NodeDiagnostics

# As https://github.com/lidatong/dataclasses-json/issues/202#issuecomment-1186373078
cfg.global_config.encoders[pathlib.Path] = str
cfg.global_config.decoders[pathlib.Path] = pathlib.Path  # is this necessary?


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
    logdir: Optional[pathlib.Path] = None

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
