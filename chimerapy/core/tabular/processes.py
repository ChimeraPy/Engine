# Subpackage management
__package__ = 'tabular'

# Built-in Imports
from typing import Any

from chimerapy.core.process import Process

class IdentityProcess(Process):
    
    def step(self, data:Any):
        return data
