# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List

class Visualization:
    """Class intended to execute after all the processes to visualize data.
    
    Attributes:
        inputs (list): A list of data stream names that are needed to
        create the visualization
        outputs (list): A list of output data streams. The list of names
        are used to create new datastreams.
    """

    def __init__(self, per_update: int, inputs: List[str], outputs: List[str]):
        self.inputs = inputs
        self.outputs = outputs
        self.per_update = per_update

    def forward(self, x):
        """Function that generates the visualization.

        Raises:
            NotImplementedError: This function is needed to be overwritten
            in the implementation class that inherets the Visualization
            class.
        """
        raise NotImplementedError


