# Third-party Imports
import numpy as np
import cv2

# PyQt5 Imports
from PyQt5 import QtCore, QtGui, QtQuick
from PyQt5.QtCore import pyqtProperty

# Internal Imports

class ContentImage(QtQuick.QQuickPaintedItem):

    imageChanged = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        # self.setRenderTarget(QtQuick.QQuickPaintedItem.FramebufferObject)
        self._image = QtGui.QImage()

    def paint(self, painter):

        if self._image.isNull():
            return

        image = self._image.scaled(self.size().toSize())
        painter.drawImage(QtCore.QPoint(), image)

    # @pyqtProperty(QtGui.QImage, notify=imageChanged)
    def image(self):
        return self._image

    # @image.setter
    def setImage(self, image):

        if self._image == image:
            return

        self._image = image
        self.imageChanged.emit()
        self.update()

    # Has to be define like this, not decorators!
    image = QtCore.pyqtProperty(QtGui.QImage, fget=image, fset=setImage, notify=imageChanged)
