from pathlib import Path

from chimerapy.manager import Manager
from .conftest import TEST_DATA_DIR


def test_manager_logdir_string():
    manager = Manager(logdir=str(TEST_DATA_DIR), port=0)
    assert manager.logdir.parent == TEST_DATA_DIR
    manager.shutdown()


def test_manager_logdir_path():
    manager = Manager(logdir=TEST_DATA_DIR, port=0)
    assert manager.logdir.parent == TEST_DATA_DIR
    manager.shutdown()
