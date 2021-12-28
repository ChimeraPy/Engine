"""Module focused on video process implementations.

Contains the following classes:
    ``Identity``

"""

# Subpackage management
__package__ = 'tabular'

# Built-in Imports
from typing import Any

from pymmdt.process import Process

class IdentityProcess(Process):
    
    def step(self, data:Any):
        return data
