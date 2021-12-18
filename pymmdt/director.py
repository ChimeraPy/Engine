"""Module focused on ``Directory`` implementation.

Contains the following classes:
    ``Director``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence
import collections

# Third Party Imports
import pandas as pd

# Internal Imports
from .session import Session
from .runner import Runner

class Director:
    """Teamwork Director between Runners."""

    def __init__(
            self,
            runners: Sequence[Runner],
            session: Session
        ) -> None:

        # Store the runners
        self.runners = runners
        self.session = session

    def start(self):

        # Create a global timetrack from all the runners' timetracks
        all_timelines = []
        for runner in self.runners:
            # Extract the runner's collector's global_timetrack
            copy_timeline = runner.collector.global_timetrack.copy()
            copy_timeline['runner_type'] = [runner.name for x in range(len(copy_timeline))]

            # Append data
            all_timelines.append(copy_timeline)

        # Concat all the timeline data frames
        self.global_timetrack: pd.DataFrame = pd.concat(all_timelines, axis=0)
        
        # Sort by the time - only if there are more than 1 data stream
        if len(self.global_timetrack) > 1:
            self.global_timetrack.sort_values(by='time', inplace=True)

    def end(self):

        # End the runners
        for runner in self.runners:
            runner.end()

    def run(self):
    
        # Execute the start setup
        self.start()

        # Then execute the runners by step
        # TODO!

        # End
        self.end()
