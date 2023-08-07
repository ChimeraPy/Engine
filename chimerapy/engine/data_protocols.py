import datetime
from typing import Dict
from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin


@dataclass
class NodePubEntry(DataClassJsonMixin):
    ip: str
    port: int


@dataclass
class NodePubTable(DataClassJsonMixin):
    table: Dict[str, NodePubEntry] = field(default_factory=dict)


@dataclass
class NodeDiagnostics(DataClassJsonMixin):
    timestamp: str = field(
        default_factory=lambda: str(datetime.datetime.now().isoformat())
    )  # ISO str
    latency: float = 0  # ms
    payload_size: float = 0  # KB
    memory_usage: float = 0  # KB
    cpu_usage: float = 0  # percentage
    num_of_steps: int = 0
