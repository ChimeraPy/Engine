# populate fields for >>>help(imagezmq)
from .__version__ import __title__, __description__, __url__, __version__
from .__version__ import __author__, __author_email__, __license__
from .__version__ import __copyright__

# Package Setup
from . import _logger

_logger.setup()

# Handling the configuration
from . import config

# Interal Imports
from .manager import Manager
from .node import Node
from .worker import Worker
from .graph import Graph

# Others
from .networking import Server, Client, Publisher, Subscriber, enums, DataChunk
from .data_handlers import SaveHandler

# Then define the records
from . import records

# Then define the entry points
from . import entry
from . import utils

# Debugging tools
from ._debug import debug
from .logreceiver import LogReceiver
