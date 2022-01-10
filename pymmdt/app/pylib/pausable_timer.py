# Built-in Imports
import time

# PyQt5 Imports
from PyQt5.QtCore import QTimer

# From: https://www.riverbankcomputing.com/pipermail/pyqt/2004-July/008325.html
class QPausableTimer (QTimer):

    def __init__ (self):
        QTimer.__init__ (self)

    def pause(self):
        if self.isActive():
          self.stop()

    def resume(self):
        if not self.isActive():
            self.start()
