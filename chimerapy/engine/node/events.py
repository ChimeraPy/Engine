from dataclasses import dataclass
from typing import Dict, Any

from ..networking.client import Client
from ..networking.data_chunk import DataChunk
from ..data_protocols import NodePubTable, NodeDiagnostics
from ..states import NodeState


@dataclass
class NodeStateChangedEvent:
    state: NodeState


@dataclass
class NewInBoundDataEvent:
    data_chunks: Dict[str, DataChunk]


@dataclass
class NewOutBoundDataEvent:
    data_chunk: DataChunk


@dataclass
class ProcessNodePubTableEvent:
    node_pub_table: NodePubTable


@dataclass
class RegisteredMethodEvent:
    method_name: str
    params: Dict[str, Any]
    client: Client


@dataclass
class GatherEvent:
    client: Client


@dataclass
class DiagnosticsReportEvent:  # diagnostics_report
    diagnostics: NodeDiagnostics
