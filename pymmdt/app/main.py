# Built-in Imports
import sys
import os
import argparse
import pathlib

# PyQt5 Imports
from PyQt5.QtWidgets import QApplication
from PyQt5.QtQml import QQmlApplicationEngine, qmlRegisterType

# Importing Model data
from pymmdt.app.pylib import DashboardModel, Backend

def main():
    # Create arguments for the application
    # parser = argparse.ArgumentParser(description="PyMMDT Dashboard CI Tool")
    # parser.add_argument("--logdir", dest="logdir", type=str, required=True, help="Path to pymmdt-generated data files.")
    # args = parser.parse_args()

    # # Convert relative path to absolute paths
    # logdir_path = pathlib.Path(args.logdir)
    # if not logdir_path.is_absolute():
    #     args.logdir = pathlib.Path.cwd() / args.logdir

    # Start the QApplication and Engine
    # app = QApplication([sys.argv[0]])
    app = QApplication(sys.argv)
    engine = QQmlApplicationEngine()

    # Adding custom data
    qmlRegisterType(DashboardModel, 'Dashboard', 1, 0, 'DashboardModel')
    engine.load(os.path.join(os.path.dirname(__file__), "qml/main.qml"))

    # # Adding backend after applying the arguments
    backend = Backend('a')
    engine.rootObjects()[0].setProperty('backend', backend)
    backend.update()

    # Running the app
    if not engine.rootObjects():
        sys.exit(-1)

    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
