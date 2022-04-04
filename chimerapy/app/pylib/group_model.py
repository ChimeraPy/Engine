# Built-in Imports
from typing import Optional
import os
import pathlib

# Third-party Imports
import pandas as pd
import numpy as np

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt
from PyQt5 import QtGui

# Internal Imports
from .qtools import toQImage

# Constants
FILE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
RESOURCE_DIR = FILE_DIR.parent / 'qml' / 'resources'

class GroupModel(QAbstractListModel):
    """Data Model for tracking groups and their entry data.
    
    This is the second level of the dashboard model. The ``GroupModel``
    is directly link to how content in the dashboard is visualize and 
    displayed.

    """

    # Role tagging
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
        """Construct the ``GroupModel``.

        Args:
            entries (Optional[pd.DataFrame]): The entries for this 
            specific group.
        """
        super().__init__()

        # Store Entries
        if type(entries) != type(None):
       
            # Creating default empty content
            content = []
            for index, row in entries.iterrows():
               
                # Setting the Empty default
                if row['dtype'] == 'image':
                    entry_content = QtGui.QImage(str(RESOURCE_DIR/'default_image.jpg'))
                elif row['dtype'] == 'video':
                    entry_content = QtGui.QImage(str(RESOURCE_DIR/'default_image.jpg'))
                else:
                    raise RuntimeError("Invalid entry dtype.")

                content.append(entry_content)

            # Appending it to the entries
            self._entries: pd.DataFrame = entries
            self._entries['content'] = content

        else:
            self._entries: pd.DataFrame = pd.DataFrame({})
            self.content = []

    def rowCount(self, parent):
        """PyQt5 required function to know the number of data entries."""
        return len(self._entries)

    def data(self, index, role=None):
        """PyQt5 required function to retrieve data from the model.

        Args:
            index: PyQt5 index.
            role: The attribute from the index to obtain from.

        """
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
        """Update the content.

        Args:
            user: The name of the user.
            entry_name: The name of the entry.
            content: The content itself.

        """
        # Obtain the specific entry that matches the user and the type
        specific_entry = (self._entries['user'] == user) & (self._entries['entry_name'] == entry_name)
        specific_entry_idx = np.where(specific_entry)[0][0]

        # Ensure the content matches the required QObject requirements
        entry_type = self._entries['dtype'].iloc[specific_entry_idx]

        # Converting the content to a QImage
        qcontent = toQImage(content)

        # Then update that one
        # print(f"Updating: {specific_entry_idx} - {user} - {entry_name} - with {qcontent}")
        self._entries.at[specific_entry_idx, 'content'] = qcontent

        # Sending signal to update content
        index = self.index(specific_entry_idx, 0)
        self.dataChanged.emit(index, index, [])

    def reset_content(self):
        """Reset all content by using the default black image."""
        
        # Placing all black images into the content
        self._entries['content'] = QtGui.QImage(str(RESOURCE_DIR/'default_image.jpg'))

        # Updating all contents through the signal
        for i in range(len(self._entries)):
            index = self.index(i, 0)
            self.dataChanged.emit(index, index, [])

    def roleNames(self):
        """PyQt5 required function to inform the roles of the model."""
        return self._roles
