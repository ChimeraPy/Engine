"""ChimeraPy Package.

ChimeraPy is a package that focus on multimodal data analytics and 
visualization.

"""

# Adding the path of ChimeraPy to PATH
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Not exposed
# from .loader import Loader
# from .logger import Logger
# from .entry import Entry
# from .collector import Collector
# from .session import Session

# Exposed
from .pipeline import Pipeline
from .process import Process
from .data_stream import DataStream
from .data_source import DataSource, Sensor, Api
from .tabular import TabularDataStream
from .video import VideoDataStream
