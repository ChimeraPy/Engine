# Third-party Imports
import numpy as np
import cv2

# PyQt5 Imports
from PyQt5 import QtCore, QtGui, QtQuick
from PyQt5.QtCore import pyqtProperty

# Internal Imports

class ContentImage(QtQuick.QQuickPaintedItem):
    """This is the painted item that is the image in the Dashboard."""

    # Signals
    imageChanged = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        """Constructing the ``ContentImage`` and saving an image."""
        super().__init__(parent)
        self._image = QtGui.QImage()

    def paint(self, painter):
        """Paint method from PyQt5 that paints the graphical element.

        In this method, we have to use our acquired image and draw as 
        an element in the dashboard.

        Args:
            painter: Qt Painter

        """
        if self._image.isNull():
            return

        # Expand the image to match the size
        # image = self._image.scaled(self.size().toSize())
        img_h, img_w = self._image.height(), self._image.width()
        canvas_h, canvas_w = self.height(), self.width()

        # Calculate the ratios
        h_ratio = canvas_h / img_h
        w_ratio = canvas_w / img_w

        # First, determine which dimensions needs to be expanded
        if h_ratio * img_w < canvas_w: # Expand height
            ratio = h_ratio
        else: # Expand width
            ratio = w_ratio

        # Then apply the expansion
        image = self._image.scaled(int(ratio * img_w), int(ratio * img_h))

        # Then, mirror the image (for some reason, the painter automatically
        # mirrors to image so we have to undo this.)
        # image = image.mirrored(horizontal=True, vertical=False)

        # Finally, update the image by the painter
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
