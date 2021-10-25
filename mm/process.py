from typing import List, Optional
from .data_stream import OfflineDataStream
from .data_sample import DataSample

class Process:
    """Generic class that compartmentalizes computational steps for a datastream."""

    def __init__(self, inputs: List[str], output: str):
        self.inputs = inputs
        self.output = output
    
    def forward(self, x:DataSample) -> DataSample:
        """A step where an data sample is used as input for the process.

        Args:
            x: a sample (of any type) in to the process.

        Raises:
            NotImplementedError: forward method needs to be overwritten.
        """
        raise NotImplementedError("forward method needs to be implemented.")
