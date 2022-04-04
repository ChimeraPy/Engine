# Package Management
__package__ = 'chimerapy'

# Built-in Imports
from typing import Dict

class Entry:
    """Abstract Entry to be saved in the session and tracking changes."""

    def __repr__(self):
        """String representation of ``Entry``."""
        return f"{self.__class__.__name__} <name={self.name}>"
    
    def __str__(self):
        """String representation of ``Entry``."""
        return self.__repr__()

    def append(self, data_chunk:Dict):
        """Append data to the entry - recording as unsaved changes.

        Args:
            data_chunk (Dict): Data chunk to be appended to entry data.

        """
        # Get the dataframe
        df = data_chunk['data']
        # Append the dataframe
        self.unsaved_changes = self.unsaved_changes.append(df)
    
    def flush(self):
        """Write/Save changes and mark them as processed.

        Raises:
            NotImplementedError: ``Entry`` is an abstract class. The 
            ``flush`` needs to be implemented in concrete classes.

        """
        raise NotImplementedError("``flush`` needs to be implemented.")

    def close(self):
        """Write/Save changes and mark them as processed.

        Raises:
            NotImplementedError: ``Entry`` is an abstract class. The 
            ``close`` needs to be implemented in concrete classes.

        """
        raise NotImplementedError("``close`` needs to be implemented.")
