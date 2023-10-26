import pathlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..data_protocols import NodeDiagnostics, NodePubTable
from ..networking.client import Client
from ..networking.data_chunk import DataChunk


@dataclass
class EnableDiagnosticsEvent:  # enable_diagnostics
    enable: bool


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


@dataclass
class Artifact:
    name: str
    path: pathlib.Path
    mime_type: str
    size: Optional[int] = None
    glob: Optional[str] = None
