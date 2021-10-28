from typing import Any

from .data_sample import DataSample
from .process import Process

class Session:
    """Data Storage that contains the latest version of all data types."""

    def __init__(self):
        self.records = {}

    def update(self, sample: DataSample) -> None:
        self.records[sample.dtype] = sample

    def apply(self, process: Process) -> DataSample:
        
        # First obtain the inputs required for the process
        inputs = [self.records[x] for x in process.inputs]

        # Passing the inputs to the process
        output = process.forward(*inputs)

        # Store the output of the process to the session
        self.update(output)

        return output
