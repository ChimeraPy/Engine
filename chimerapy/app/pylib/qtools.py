# Third-party Imports
import numpy as np
import cv2

# PyQt5 Imports
from PyQt5 import QtGui

# Constants
GRAY_COLOR_TABLE = [QtGui.qRgb(i, i, i) for i in range(256)]

def toQImage(im:np.ndarray) -> QtGui.QImage:
    """Convert a numpy image into a PyQt5 QImage.

    Args:
        im (np.ndarray): The inputted numpy image.

    Returns:
        QtGui.QImage: The transformed image as a PyQt5 QImage.

    """
    if im is None:
        return QtGui.QImage()

    if im.dtype == np.uint8:
        if len(im.shape) == 2:
            qim = QtGui.QImage(im.data, im.shape[1], im.shape[0], im.strides[0], QtGui.QImage.Format_Indexed8)
            qim.setColorTable(GRAY_COLOR_TABLE)
            return qim.copy()

        elif len(im.shape) == 3:
            if im.shape[2] == 3:
                w, h, _ = im.shape
                # rgb_image = cv2.cvtColor(im, cv2.COLOR_BGR2RGB)
                # flip_image = cv2.flip(rgb_image, 1)
                qim = QtGui.QImage(im.data, h, w, QtGui.QImage.Format_RGB888)
                return qim.copy()

    return QtGui.QImage()
