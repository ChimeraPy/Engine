# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtSignal

class Backend(QObject):

    def __init__(self, args):
        super().__init__()

        # Store the CI arguments
        self.args = args

    def update(self):
        ...
