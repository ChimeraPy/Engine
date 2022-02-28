# Package Management
__package__ = 'pymmdt'

# Built-in Imports

# Third-party Imports
import pandas as pd

class Entry:
    """Abstract Entry to be saved in the session and tracking changes."""

    def __repr__(self):
        return f"{self.__class__.__name__} <name={self.name}>"
    
    def __str__(self):
        return self.__repr__()

    def append(self, data_chunk:dict):
        # Get the dataframe
        df = data_chunk['data']
        # Append the dataframe
        self.unsaved_changes = self.unsaved_changes.append(df)
    
    # def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta):

    #     # Ensure that any new changes have been dedicated to memory
    #     self.flush()

    #     # Then call the data stream, get their data, and return it
    #     data = self.stream.get(start_time, end_time)

    #     return data

    def flush(self):
        """Write/Save changes and mark them as processed.

        Raises:
            NotImplementedError: ``Entry`` is an abstract class. The 
            ``flush`` needs to be implemented in concrete classes.

        """
        raise NotImplementedError("``flush`` needs to be implemented.")

    def close(self):

        self.flush()
        # self.stream.close()

        return None
