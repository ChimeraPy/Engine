# Built-in Imports

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

# Interal Imports
from .timeline_model import TimelineModel

class TimetrackModel(QAbstractListModel):
    """Main model for the timetrack. 

    This model uses multiple ``TimelineModel``s to model multiple users.
    
    """
   
    # Role tagging
    UserRole = Qt.UserRole + 1
    GroupRole = Qt.UserRole + 2

    _roles = {
        UserRole: b"user",
        GroupRole: b"timelines"
    }
    
    def __init__(self):
        """Construct the ``TimetrackModel``.
        """
        super().__init__()

        # Default empty values for essential variables.
        self.entries = pd.DataFrame()
        self.app_start_time = pd.Timedelta(seconds=0)
        self.app_end_time = pd.Timedelta(seconds=0)
        self.timeline_group_names = []
        self.timeline_groups = []

    def update_data(
            self, 
            entries:pd.DataFrame, 
            app_start_time:pd.Timedelta, 
            app_end_time:pd.Timedelta
        ):
        """Update the timetrack's meta data.

        The entries contain the independent timeline's start and end
        time information. In conjuction with the total application
        start and end time, a timetrack of the entire session is 
        possible.

        Args:
            entries (pd.DataFrame): Data frame of entries' information.
            app_start_time (pd.Timedelta): Global start time.
            app_end_time (pd.Timedelta): Global end time.

        """
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
        """PyQt5 required function to report number of data elements."""
        return len(self.timeline_groups)

    def data(self, index, role=None):
        """PyQt5 required function to retrieve data from the model.

        Args:
            index: PyQt5 index.
            role: The attribute to be access for a data row.

        """
        row = index.row()
        if role == self.UserRole:
            return self.timeline_group_names[row]
        if role == self.GroupRole:
            return self.timeline_groups[row]

        return None

    def roleNames(self):
        """PyQt5 required function to report the roles for the model."""
        return self._roles
