from typing import List, Optional

class Process:

    def __init__(self, inputs: List[str], outputs: List[str]):
        self.inputs = inputs
        self.outputs = outputs
    
    def forward(self, x):
        raise NotImplementedError

