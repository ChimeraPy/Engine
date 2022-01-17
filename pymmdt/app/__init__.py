# Built-in Imports
import os

# Fix some environmental variables caused by import cv2 alongside PyQt5
# More infomation can be found here: https://forum.qt.io/topic/119109/using-pyqt5-with-opencv-python-cv2-causes-error-could-not-load-qt-platform-plugin-xcb-even-though-it-was-found/24
for k, v in os.environ.items():
    if k.startswith("QT_") and "cv2" in v:
        del os.environ[k]

# Import models
from .main import application_setup
