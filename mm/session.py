from typing import Optional

from .data_sample import DataSample
from .process import Process

class Session:
    """Data Storage that contains the latest version of all data types."""

    def __init__(self):
        self.records = {}

    def update(self, sample: DataSample) -> None:
        # Add the sample to the session data
        self.records[sample.dtype] = sample
        
    def apply(self, process: Process) -> Optional[DataSample]:

        # Before obtainin the needed inputs, determine first if there
        # is the needed inputs
        inputs_missing = [x not in self.records for x in process.inputs]
      
        # Even if one input is missing, skip it
        if any(inputs_missing):
            return None

        # First obtain the inputs required for the process
        inputs = [self.records[x] for x in process.inputs]

        # Passing the inputs to the process
        output = process.forward(*inputs)

        # Store the output of the process to the session
        if isinstance(output, DataSample):
            self.update(output)

        return output
