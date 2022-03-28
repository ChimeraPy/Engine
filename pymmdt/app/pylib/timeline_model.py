# Built-in Imports
from typing import Optional

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

class TimelineModel(QAbstractListModel):
    """The model to represent single entry timelines.

    This is the second level model for the timetrack. The 
    ``TimetrackModel`` uses multiple ``TimelineModel`` to represent the
    collective timelines of all entries.

    """

    # Role tagging
    EntryRole = Qt.UserRole + 1
    UserRole = Qt.UserRole + 2
    DTypeRole = Qt.UserRole + 3

    _roles = {
        EntryRole: b"entry_name",
        UserRole: b"user",
        DTypeRole: b"dtype"
    }

    def __init__(
            self, 
            entries:Optional[pd.DataFrame]=None
        ):
        """Construct the ``TimelineModel`` instance for the entries.

        Args:
            entries (Optional[pd.DataFrame]): entries
        """
        super().__init__()

        # Store Entries
        if type(entries) != type(None):
            self._entries: pd.DataFrame = entries
        else:
            self._entries: pd.DataFrame = pd.DataFrame({})

    def rowCount(self, parent):
        """PyQt5 required function to report number of data elements."""
        return len(self._entries)

    def data(self, index, role=None):
        """PyQt5 required function to retrieve data from the model.

        Args:
            index: PyQt5 index.
            role: The attribute to be access for a data row.

        """
        row = index.row()
        if role == self.EntryRole:
            return self._entries.iloc[row]["entry_name"]
        if role == self.UserRole:
            return self._entries.iloc[row]["user"]
        if role == self.DTypeRole:
            return self._entries.iloc[row]["dtype"]

    def roleNames(self):
        """PyQt5 required function to report the roles for the model."""
        return self._roles
