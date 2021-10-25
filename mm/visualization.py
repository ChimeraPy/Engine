# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List, Optional

class Visualization:
    """Class intended to execute after all the processes to visualize data.
    
    Attributes:
        inputs (list): A list of data stream names that are needed to
        create the visualization
        output (str): the name of the output stream from the process
    """

    def __init__(self, inputs: List[str], output: Optional[str]):
        self.inputs = inputs
        self.output = output

    def forward(self, x):
        """Function that generates the visualization.

        Raises:
            NotImplementedError: This function is needed to be overwritten
            in the implementation class that inherets the Visualization
            class.
        """
        raise NotImplementedError


