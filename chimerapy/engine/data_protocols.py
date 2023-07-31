from dataclasses import dataclass, field
from typing import Dict

from dataclasses_json import DataClassJsonMixin


@dataclass
class NodePubEntry(DataClassJsonMixin):
    ip: str
    port: int


@dataclass
class NodePubTable(DataClassJsonMixin):
    table: Dict[str, NodePubEntry] = field(default_factory=dict)
