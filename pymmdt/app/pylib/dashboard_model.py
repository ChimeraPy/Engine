# Resource:
#https://wiki.qt.io/Selectable-list-of-Python-objects-in-QML
#https://github.com/cedrus/qt/blob/master/qtdeclarative/src/qml/doc/snippets/qml/listmodel/listmodel-nested.qml
#https://stackoverflow.com/questions/4303561/pyqt-and-qml-how-can-i-create-a-custom-data-model

# Built-in Imports
from typing import List

# PyQt5 Imports
from PyQt5.QtCore import QAbstractListModel, Qt

# Internal Imports
from .modality_model import ModalityModel

class DashboardModel(QAbstractListModel):

    DTypeRole = Qt.UserRole + 1
    DModalityRole = Qt.UserRole + 2

    _roles = {
        DTypeRole: b"dtype",
        DModalityRole: b"modality"
    }

    def __init__(self, modalities:List[ModalityModel]=None):
        super().__init__()

        # Store modalities
        if modalities:
            self._modalities = modalities
        else:
            self._modalities = []

        # Test Case
        self._modalities = [
            ModalityModel(dtype="video"),
            ModalityModel(dtype="image")
        ]

    def rowCount(self, parent):
        return len(self._modalities)

    def data(self, index, role=None):
        row = index.row()
        if role == self.DTypeRole:
            return self._modalities[row].dtype
        if role == self.DModalityRole:
            return self._modalities[row]

        return None

    def roleNames(self):
        return self._roles
