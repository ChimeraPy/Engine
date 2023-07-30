from dataclasses import dataclass, field
from typing import Dict, Any
from enum import Enum

from ..node.node_config import NodeConfig


@dataclass
class BroadcastEvent:
    signal: Enum
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CreateNodeEvent:
    node_config: NodeConfig


@dataclass
class DestroyNodeEvent:
    node_id: str


@dataclass
class ProcessNodeServerDataEvent:
    msg: Dict


@dataclass
class RegisteredMethodEvent:
    node_id: str
    method_name: str
    params: Dict[str, Any]


@dataclass
class UpdateGatherEvent:
    node_id: str
    latest_value: Any


@dataclass
class UpdateResultsEvent:
    node_id: str
    output: Any
