# Resource:
#https://wiki.qt.io/Selectable-list-of-Python-objects-in-QML
#https://github.com/cedrus/qt/blob/master/qtdeclarative/src/qml/doc/snippets/qml/listmodel/listmodel-nested.qml
#https://stackoverflow.com/questions/4303561/pyqt-and-qml-how-can-i-create-a-custom-data-model

# Built-in Imports
import datetime

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

# Internal Imports
from .group_model import GroupModel

class DashboardModel(QAbstractListModel):

    SortByRole = Qt.UserRole + 1
    GroupRole = Qt.UserRole + 2

    _roles = {
        SortByRole: b"sort_by",
        GroupRole: b"group"
    }

    def __init__(self): #, entries:List[ModalityModel]=None):
        super().__init__()

        self.entries = pd.DataFrame()
        self.groups = []
        self._sort_by = 'entry_name'

    def sort_by(self, by):
        assert by in self.entries.columns

    def update_data(self, entries):

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

    def update_content(self, index, user, entry_name, content):
  
        # First, determine which group by the sort_by
        if self._sort_by == 'entry_name':
            group_idx = self.unique_groups_tags.index(entry_name)
        elif self._sort_by == 'user_name':
            group_idx = self.unique_groups_tags.index(user)
        else:
            raise RuntimeError("Invalid _sort_by type for DashboardModel.")

        # Then update the content for that group
        # print(f"Job ID: {index} - g{group_idx} - - {user} - {entry_name} - time: {datetime.datetime.now()}")
        self.groups[group_idx].update_content(user, entry_name, content)

    def rowCount(self, parent):
        return len(self.groups)

    def data(self, index, role=None):
        row = index.row()
        if role == self.SortByRole:
            return self.unique_groups_tags[row]
        if role == self.GroupRole:
            return self.groups[row]

        return None

    def roleNames(self):
        return self._roles
