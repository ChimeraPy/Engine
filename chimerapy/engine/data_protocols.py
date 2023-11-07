import datetime
import enum
import logging
import pathlib
import typing
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Union

if typing.TYPE_CHECKING:
    from .graph import Graph
    from .node.node_config import NodeConfig
    from .states import NodeState

from dataclasses_json import DataClassJsonMixin

from .networking import DataChunk


@dataclass
class NodePubEntry(DataClassJsonMixin):
    ip: str
    port: int


@dataclass
class NodePubTable(DataClassJsonMixin):
    table: Dict[str, NodePubEntry] = field(default_factory=dict)


@dataclass
class NodeDiagnostics(DataClassJsonMixin):
    node_id: str = ""
    timestamp: str = field(
        default_factory=lambda: str(datetime.datetime.now().isoformat())
    )  # ISO str
    latency: float = 0  # ms
    payload_size: float = 0  # KB
    memory_usage: float = 0  # KB
    cpu_usage: float = 0  # percentage
    num_of_steps: int = 0


########################################################################
## Manager specific
########################################################################


@dataclass
class RegisterMethodResponseData(DataClassJsonMixin):
    success: bool
    result: Dict[str, Any]


@dataclass
class UpdateSendArchiveData(DataClassJsonMixin):
    worker_id: str
    success: bool


@dataclass
class CommitData(DataClassJsonMixin):
    graph: "Graph"
    mapping: Dict[str, List[str]]
    context: Literal["multiprocessing", "threading"] = "multiprocessing"
    send_packages: Optional[List[Dict[str, Any]]] = None


########################################################################
## Worker specific
########################################################################


@dataclass
class ConnectData(DataClassJsonMixin):
    method: Literal["ip", "zeroconf"]
    host: Optional[str] = None
    port: Optional[int] = None


@dataclass
class GatherData(DataClassJsonMixin):
    node_id: str
    output: Union[DataChunk, List[int], str]


@dataclass
class ResultsData(DataClassJsonMixin):
    node_id: str
    success: bool
    output: Any


@dataclass
class ServerMessage(DataClassJsonMixin):
    signal: enum.Enum
    data: Dict[str, Any] = field(default_factory=dict)
    client_id: Optional[str] = None


@dataclass
class WorkerInfo(DataClassJsonMixin):
    host: str
    port: int
    logdir: pathlib.Path
    node_config: "NodeConfig"
    config: Dict[str, Any] = field(default_factory=dict)
    logging_level: int = logging.INFO
    worker_logging_port: Optional[int] = None


########################################################################
## Node specific
########################################################################


@dataclass
class PreSetupData(DataClassJsonMixin):
    state: "NodeState"
    logger: logging.Logger


@dataclass
class RegisteredMethod(DataClassJsonMixin):
    name: str
    style: Literal["concurrent", "blocking", "reset"] = "concurrent"
    params: Dict[str, str] = field(default_factory=dict)


@dataclass
class RegisteredMethodData(DataClassJsonMixin):
    node_id: str
    method_name: str
    params: Dict[str, Any] = field(default_factory=dict)
