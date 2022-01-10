# Built-in Imports
from typing import Optional

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

class TimelineModel(QAbstractListModel):

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
        super().__init__()

        # Store Entries
        if type(entries) != type(None):
            self._entries: pd.DataFrame = entries
        else:
            self._entries: pd.DataFrame = pd.DataFrame({})

    def rowCount(self, parent):
        return len(self._entries)

    def data(self, index, role=None):
        row = index.row()
        if role == self.EntryRole:
            return self._entries.iloc[row]["entry_name"]
        if role == self.UserRole:
            return self._entries.iloc[row]["user"]
        if role == self.DTypeRole:
            return self._entries.iloc[row]["dtype"]

    def roleNames(self):
        return self._roles
