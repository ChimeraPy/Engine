from pathlib import Path

from chimerapy.manager import Manager


def test_manager_logdir_string():
    manager = Manager(logdir=".", port=0)
    assert manager.logdir.parent == Path(".").resolve()
    manager.shutdown()


def test_manager_logdir_path():
    manager = Manager(logdir=Path("."), port=0)
    assert manager.logdir.parent == Path(".").resolve()
    manager.shutdown()
