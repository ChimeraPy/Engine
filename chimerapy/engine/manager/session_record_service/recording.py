from typing import Optional

import datetime


class Recording:
    def __init__(self, uuid: str):

        # Save parameters
        self.uuid = uuid

        # State variables
        self.start_time = datetime.datetime.now()
        self.stop_time: Optional[datetime.datetime] = None

    def stop(self):

        # Update the stop time
        self.stop_time = datetime.datetime.now()
