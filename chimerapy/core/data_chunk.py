import multiprocessing as mp
from uuid import uuid4

from chimerapy.utils.memory_manager import get_memory_data_size

class DataChunk:
    """
        Data chunk represents a single data chunk with the following attributes on it.
            data:
                The actual data that needs to go in. Can be a pandas DataFrame or some other format
            window_index:
                The entire time series of data is synchronized with respect to an absolute window
                of time duration. In a sense, the window_index represents the absolute time at which
                the data chunk was created by any process. To keep everything in sync, the processes
                can't consume or produce data in case their local process time/window_index differs
                the overall window_index by an amount greater than a certain THRESHOLD.

                The THRESHOLD determination is discussed in the process's consumtion/production
                modules.
            id:
                A unique identifier that distinguishes the data chunk. For this implementation we
                will simply use the memory address of the item as it will be unique for a given run.
                UUID can be a replacement, however uuid creation is an expensive process.
    """

    def __init__(self, owner: str, data, time_stamp=0) -> None:
        self.data = data
        self.owner = owner
        self.time_stamp = time_stamp
        self.id = id(self)
        self.memory_consumed = None
        self.calculate_consumed_memory()

    def calculate_consumed_memory(self):
        """
            Calcuate the consumed memory.
            Note the slight inaccuracy because it updates itself after calculating, which may trigger
            change. However, for the given use case, the memory limit will be soft and not a hard one.
        """
        if not self.memory_consumed:
            self.memory_consumed = get_memory_data_size(self)

        return self.memory_consumed
