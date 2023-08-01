from dataclasses import dataclass
from typing import Dict

from ..networking.data_chunk import DataChunk


@dataclass
class NewInBoundDataEvent:
    data: Dict[str, DataChunk]


@dataclass
class NewOutBoundDataEvent:
    data_chunk: DataChunk
