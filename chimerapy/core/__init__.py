"""ChimeraPy Package.

ChimeraPy is a package that focus on multimodal data analytics and 
visualization.

"""

# Adding the path of ChimeraPy to PATH
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Exposed
from .process import Process
from .reader import Reader
from .writer import Writer

from .data_stream import DataStream
from .data_source import DataSource, Sensor, Api
from .tabular import TabularDataStream
from .video import VideoDataStream

from .collector import Collector
from .session import Session

from .pipeline import Pipeline
