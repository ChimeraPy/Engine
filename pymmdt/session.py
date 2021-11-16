"""Module focused on the ``Session`` implementation.

Contains the following classes:
    ``Session``
"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Optional, Dict, Type

# Internal Imports
from .data_sample import DataSample
from .data_stream import DataStream
from .process import Process

class Session:
    """Data Storage that contains the latest version of all data types.

    Attributes:
        records (Dict[str, DataSample]): Stores the latest version of a ``data_type`` sample
        or the output of a process.

    Todo:
        * Allow the option to store the intermediate samples stored in
        the session.

    """

    def __init__(self, save_streams: Optional[Dict[str, Type[DataStream]]]=None) -> None:
        """``Session`` Constructor.

        Args:
            save_streams (Dict[str, Type[DataStream]]): A dictionary of data streams
            to save into DataStream objects. The dictionary keys are 
            the names of the data streams and the value is the type of 
            data stream by passing a cls type.

        """
        # Create a record to the data
        self.records: Dict[str, DataSample] = {}

        # Create the data stream instances 
        if save_streams:
            self.save_streams: Dict[str, DataStream] = {}
            for ds_type, ds_cls in save_streams.items():
                self.save_streams[ds_type] = ds_cls.empty(name=ds_type)
        else:
            self.save_streams = {}

    def update(self, sample: DataSample) -> None:
        """Store the sample into the records.

        Args:
            sample (pymddt.DataSample): The sample to be stored in the records.

        """
        # Add the sample to the session data
        self.records[sample.dtype] = sample

        # Also, not only keep track of the latest sample but store the
        # sample if it is a data stream we are interested in saving
        if sample.dtype in self.save_streams.keys():
            self.save_streams[sample.dtype].append(sample)
        
    def apply(self, process: Process) -> Optional[DataSample]:
        """Apply the process by obtaining the necessary inputs and stores the generated output in the records.

        Args:
            process (pymddt.Process): The process to be executed with the
            records.

        """
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

    def close(self) -> None:
        """Close session.

        Todo:
            * Add an argument to session to allow the saving of the session
            values at the end.

        """
        ...
