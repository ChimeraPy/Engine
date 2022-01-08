# import sys
# import os

# # PyQt5 Imports
# # from PyQt5.QtWidgets import QApplication
# # from PyQt5.QtQml import QQmlApplicationEngine
# from PySide6.QtWidgets import QApplication
# from PySide6.QtQml import QQmlApplicationEngine
# # from PySide6.QtQuick import QQuickView

# if __name__ == "__main__":

#     app = QApplication()
#     # view = QQuickView()
#     engine = QQmlApplicationEngine()
#     engine.load(os.path.join(os.path.dirname(__file__), "qml/main.qml"))
#     # view.setSource(os.path.join(os.path.dirname(__file__), "qml/main.qml"))
#     # view.show()

#     # Running the app
#     # if not engine.rootObjects():
#     #     sys.exit(-1)

#     sys.exit(app.exec())

import os
from pathlib import Path
import sys

from PySide6.QtCore import QCoreApplication, Qt, QUrl
from PySide6.QtGui import QGuiApplication
from PySide6.QtQml import QQmlApplicationEngine

CURRENT_DIRECTORY = Path(__file__).resolve().parent


def main():
    app = QGuiApplication(sys.argv)

    engine = QQmlApplicationEngine()

    filename = os.fspath(CURRENT_DIRECTORY / "qml" / "main.qml")
    url = QUrl.fromLocalFile(filename)

    def handle_object_created(obj, obj_url):
        if obj is None and url == obj_url:
            QCoreApplication.exit(-1)

    engine.objectCreated.connect(handle_object_created, Qt.QueuedConnection)
    engine.load(url)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
