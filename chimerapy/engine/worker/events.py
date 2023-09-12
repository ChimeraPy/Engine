import pathlib
from dataclasses import dataclass, field
from typing import Dict, Any
from enum import Enum

from ..node.node_config import NodeConfig
from ..data_protocols import NodePubTable


@dataclass
class EnableDiagnosticsEvent:
    enable: bool


@dataclass
class BroadcastEvent:
    signal: Enum
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SendMessageEvent:
    client_id: str
    signal: Enum
    data: Dict[str, Any]


@dataclass
class CreateNodeEvent:
    node_config: NodeConfig


@dataclass
class DestroyNodeEvent:
    node_id: str


@dataclass
class ProcessNodePubTableEvent:
    node_pub_table: NodePubTable


@dataclass
class RegisteredMethodEvent:
    node_id: str
    method_name: str
    params: Dict[str, Any]


@dataclass
class UpdateGatherEvent:
    node_id: str
    gather: Any


@dataclass
class UpdateResultsEvent:
    node_id: str
    results: Any


@dataclass
class SendArchiveEvent:
    path: pathlib.Path
