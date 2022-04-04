# PyQt5 Imports
from PyQt5.QtCore import QObject, pyqtProperty, pyqtSignal

class LoadingBarObject(QObject):
    """Simple state-based loading bar for tracking load data."""

    stateChanged = pyqtSignal(float)

    def __init__(self):
        super().__init__()
        self._state = 0

    @pyqtProperty(float, notify=stateChanged)
    def state(self):
        return self._state 

    @state.setter
    def state(self, state):
        if self._state != state:
            self._state = state 
            self.stateChanged.emit(state)
