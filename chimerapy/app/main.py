# Built-in Imports
from typing import Dict
import sys
import os
import argparse
import pathlib

# PyQt5 Imports
from PyQt5.QtWidgets import QApplication
from PyQt5.QtQml import QQmlApplicationEngine, qmlRegisterType

# Importing Model data
from chimerapy.app.pylib import Manager, ContentImage

def application_setup(args:Dict):
    """Setting the application and passing arguments to the ``Manager``.

    Args:
        args (Dict): The arguments that will be passed to the ``Manager``.

    """
    # QML Register
    qmlRegisterType(ContentImage, "chimerapy.app.pylib" , 1, 0, "ContentImage")

    # Start the QApplication and Engine
    app = QApplication([sys.argv[0]])
    engine = QQmlApplicationEngine()    

    # Constructing the Application Manager
    manager = Manager(**args)
    app.aboutToQuit.connect(manager.exit)

    # # Adding backend after applying the arguments
    engine.rootContext().setContextProperty("Manager", manager)
    engine.rootContext().setContextProperty("SlidingBar", manager.sliding_bar)
    engine.rootContext().setContextProperty("LoadingBar", manager.loading_bar)
    engine.rootContext().setContextProperty("SortingBar", manager.sorting_bar)

    # Adding custom data
    engine.load(os.path.join(os.path.dirname(__file__), "qml/main.qml"))

    return app, engine, manager

def main():
    """Entry point for the front-end application."""
    # Create arguments for the application
    parser = argparse.ArgumentParser(description="ChimeraDash CI Tool")
    parser.add_argument(
        "--logdir", dest="logdir", type=str, required=True, help="Path to pymmdt-generated data files.",
    )
    parser.add_argument(
        "--verbose", dest="verbose", type=bool, default=False, help="Verbose output for debugging."
    )
    parser.add_argument(
        "--memory_limit", dest="memory_limit", type=float, default=0.8, help="RAM memory limit percentage available for the ChimeraDash."
    )
    args = parser.parse_args()

    # Convert relative path to absolute paths
    logdir_path = pathlib.Path(args.logdir)
    if not logdir_path.is_absolute():
        args.logdir = pathlib.Path.cwd() / args.logdir

    # Convert the Namespace to a dictionary
    d_args = vars(args)

    # Setup the application
    app, engine, manager = application_setup(d_args)

    # Running the app
    if not engine.rootObjects():
        sys.exit(-1)

    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
