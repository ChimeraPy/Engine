# Built-in Imports
from typing import Optional

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

# Interal Imports
from .timeline_model import TimelineModel

class TimetrackModel(QAbstractListModel):
    
    UserRole = Qt.UserRole + 1
    GroupRole = Qt.UserRole + 2

    _roles = {
        UserRole: b"user",
        GroupRole: b"timelines"
    }
    
    def __init__(self): #, entries:List[ModalityModel]=None):
        super().__init__()

        self.entries = pd.DataFrame()
        self.app_start_time = pd.Timedelta(seconds=0)
        self.app_end_time = pd.Timedelta(seconds=0)
        self.timeline_group_names = []
        self.timeline_groups = []

    def update_data(self, entries, app_start_time, app_end_time):

        # Storing the entries
        self.entries = entries
        self.app_start_time = app_start_time
        self.app_end_time = app_end_time

        # Determine the number of unique users!
        self.timeline_group_names = self.entries['user'].unique()

        # Then organize the entries based on the user
        groups = [self.entries.loc[self.entries['user'] == x]\
                  for x in self.timeline_group_names]
        
        # Reset index and drop columns
        for i in range(len(groups)):
            groups[i].reset_index(inplace=True)
            groups[i] = groups[i].drop(columns=['index'])

        self.timeline_groups = [TimelineModel(group) for group in groups]
    
    def rowCount(self, parent):
        return len(self.timeline_groups)

    def data(self, index, role=None):
        row = index.row()
        if role == self.UserRole:
            return self.timeline_group_names[row]
        if role == self.GroupRole:
            return self.timeline_groups[row]

        return None

    def roleNames(self):
        return self._roles
