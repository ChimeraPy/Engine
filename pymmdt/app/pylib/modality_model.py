# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

class ModalityModel(QAbstractListModel):

    SourceRole = Qt.UserRole + 1
    UserRole = Qt.UserRole + 2

    _roles = {
        SourceRole: b"source",
        UserRole: b"user"
    }

    def __init__(self, dtype:str="test", entries=None):
        super().__init__()

        # Store dtype
        self.dtype = dtype

        # Store Entries
        if entries:
            self._entries = entries
        else:
            self._entries = []

        # Test Case
        self._entries = [
            {
                'user': 'P01',
                'source': 'path1'
            },
            {
                'user': 'P02',
                'source': 'path2'
            }
        ]

    def rowCount(self, parent):
        return len(self._entries)

    def data(self, index, role=None):
        row = index.row()
        if role == self.SourceRole:
            return self._entries[row]["source"]
        if role == self.UserRole:
            return self._entries[row]["user"]

    def roleNames(self):
        return self._roles
