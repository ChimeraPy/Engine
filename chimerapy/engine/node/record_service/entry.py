import datetime
import uuid
from dataclasses import dataclass, field
from typing import Union, Dict, Any

import pandas as pd
import pyaudio
import numpy as np


@dataclass
class Entry:
    name: str
    uuid: str = str(uuid.uuid4())
    timestamp: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now()
    )


@dataclass
class ImageEntry(Entry):
    data: np.ndarray = field(default_factory=lambda: np.empty((0, 0, 3)))


@dataclass
class VideoEntry(Entry):
    data: np.ndarray = field(default_factory=lambda: np.empty((0, 0, 3)))
    fps: Union[float, int] = 30


@dataclass
class AudioEntry(Entry):
    data: np.ndarray = field(default_factory=lambda: np.empty((0, 0, 3)))
    channels: int = 2
    format: int = pyaudio.paInt16
    rate: int = 44100


@dataclass
class TabularEntry(Entry):
    data: Union[Dict[str, Any], pd.Series, pd.DataFrame] = field(default_factory=dict)
