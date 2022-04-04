# Resource:
#https://wiki.qt.io/Selectable-list-of-Python-objects-in-QML
#https://github.com/cedrus/qt/blob/master/qtdeclarative/src/qml/doc/snippets/qml/listmodel/listmodel-nested.qml
#https://stackoverflow.com/questions/4303561/pyqt-and-qml-how-can-i-create-a-custom-data-model

# Built-in Imports
import datetime # use for debugging

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

# Internal Imports
from .group_model import GroupModel

class DashboardModel(QAbstractListModel):
    """Data Model that tracks the dashboard.

    This is the base level of the data model for the dashboard. An 
    additional level is perform by the ``GroupModel``. The dashboard
    model focuses on the ``user`` and ``entry_name`` separation.

    """
   
    # Role tagging
    SortByRole = Qt.UserRole + 1
    GroupRole = Qt.UserRole + 2

    _roles = {
        SortByRole: b"sort_by",
        GroupRole: b"group"
    }

    def __init__(self):
        super().__init__()

        # Creating empty essential variables.
        self.entries = pd.DataFrame()
        self.groups = []
        self._sort_by = None

    def update_data(self, entries:pd.DataFrame, sort_by:str):
        """Updating data organized by the entries and sorting preference.

        Args:
            entries (pd.DataFrame): Data frame containing detailed
            information of the entries.

            sort_by (str): Flag to indicate if sorting by ``user`` or 
            ``entry_name``.
        """

        # Reset model if different sort_by
        if self._sort_by != sort_by:
            model_should_be_resetted = True
        else:
            model_should_be_resetted = False

        # Update the sort_by
        self._sort_by = sort_by

        # Storing the entries
        self.entries = entries
    
        # Split the dataframes based on the sort_by
        self.unique_groups_tags = self.entries[self._sort_by].unique().tolist()
        groups = [self.entries.loc[self.entries[self._sort_by] == x]\
                  for x in self.unique_groups_tags]

        # Reset index and drop columns
        for i in range(len(groups)):
            groups[i].reset_index(inplace=True)
            groups[i] = groups[i].drop(columns=['index'])

        # Now group the entries
        self.groups = [GroupModel(group) for group in groups]
           
        # If the groups were changed, we need to reset the model
        if model_should_be_resetted:
            self.modelReset.emit()

    def update_content(self, index, user, entry_name, content):
        """Update the content given entries.

        Args:
            index: Job number.
            user: Name of the user.
            entry_name: Name of the entry.
            content: The actual content that is meant to be used.

        """
        # First, determine which group by the sort_by
        if self._sort_by == 'entry_name':
            group_idx = self.unique_groups_tags.index(entry_name)
        elif self._sort_by == 'user':
            group_idx = self.unique_groups_tags.index(user)
        else:
            raise RuntimeError("Invalid _sort_by type for DashboardModel.")

        # Then update the content for that group
        # print(f"Job ID: {index} - g{group_idx} - - {user} - {entry_name} - time: {datetime.datetime.now()}")
        self.groups[group_idx].update_content(user, entry_name, content)

    def reset_content(self):
        """Reset content by using default black image."""

        # Iterate over all groups and reset their own content
        for group in self.groups:
            group.reset_content()

    def rowCount(self, parent):
        """PyQt5 required function to now the size of the model."""
        return len(self.groups)

    def data(self, index, role=None):
        """PyQt5 required function to retrieve data in model.

        Args:
            index: PyQt5 index.
            role: The types of roles (types of data).

        """
        row = index.row()
        if role == self.SortByRole:
            return self.unique_groups_tags[row]
        if role == self.GroupRole:
            return self.groups[row]

        return None

    def roleNames(self):
        """PyQt5 required function to define the roles of the model."""
        return self._roles
