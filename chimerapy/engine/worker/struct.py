import enum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dataclasses_json import DataClassJsonMixin


@dataclass
class GatherData(DataClassJsonMixin):
    node_id: str
    gather: Any


@dataclass
class ResultsData(DataClassJsonMixin):
    node_id: str
    results: Any


@dataclass
class RegisterMethodData(DataClassJsonMixin):
    node_id: str
    method_name: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServerMessage(DataClassJsonMixin):
    signal: enum.Enum
    data: Dict[str, Any] = field(default_factory=dict)
    client_id: Optional[str] = None
