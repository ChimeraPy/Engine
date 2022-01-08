# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtSignal

class Backend(QObject):

    def __init__(self, args):
        super().__init__()

        # Store the CI arguments
        self.args = args

        # Keeping track of all the data in the logdir
        self.logdir_records = {}

    def get_meta_logdir(self):
        # Obtain all the meta files 
        ...

    def update(self):
        ...
