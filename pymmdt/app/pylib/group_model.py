# Built-in Imports
from typing import Optional

# Third-party Imports
import pandas as pd
import numpy as np

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt, QObject
from PyQt5 import QtGui

# Internal Imports
from .content import ContentImage
from .qtools import toQImage

class GroupModel(QAbstractListModel):

    EntryRole = Qt.UserRole + 1
    UserRole = Qt.UserRole + 2
    DTypeRole = Qt.UserRole + 3
    ContentRole = Qt.UserRole + 4

    _roles = {
        EntryRole: b"entry_name",
        UserRole: b"user",
        DTypeRole: b"dtype",
        ContentRole: b"content"
    }

    def __init__(
            self, 
            entries:Optional[pd.DataFrame]=None
        ):
        super().__init__()

        # Store Entries
        if type(entries) != type(None):
       
            # Creating default empty content
            content = []
            for index, row in entries.iterrows():
               
                # Setting the Empty default
                if row['dtype'] == 'image':
                    entry_content = QtGui.QImage()
                elif row['dtype'] == 'video':
                    entry_content = QtGui.QImage()
                else:
                    raise RuntimeError("Invalid entry dtype.")

                content.append(entry_content)

            # Appending it to the entries
            self._entries: pd.DataFrame = entries
            self._entries['content'] = content

            print("Initial entries")
            print(self._entries)

        else:
            self._entries: pd.DataFrame = pd.DataFrame({})
            self.content = []

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
        if role == self.ContentRole:
            return self._entries.iloc[row]['content']

    def update_content(self, user, entry_name, content):

        # Obtain the specific entry that matches the user and the type
        specific_entry = (self._entries['user'] == user) & (self._entries['entry_name'] == entry_name)
        specific_entry_idx = np.where(specific_entry)[0][0]

        # print(specific_entry_idx)

        # Ensure the content matches the required QObject requirements
        entry_type = self._entries['dtype'].iloc[specific_entry_idx]

        # print(entry_type) 

        if entry_type == 'image':
            qcontent = toQImage(content['images'])
        elif entry_type == 'video':
            qcontent = toQImage(content['frames'])
        else:
            raise RuntimeError("Invalid dtype.")

        # Then update that one
        print(f"Updating: {specific_entry_idx} - {user} - {entry_name} - with {qcontent}")
        self._entries.at[specific_entry_idx, 'content'] = qcontent

        # print(self._entries)

        # Sending signal to update content
        index = self.index(specific_entry_idx, 0)
        self.dataChanged.emit(index, index, [])

    def roleNames(self):
        return self._roles
