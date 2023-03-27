from pathlib import Path

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp
from chimerapy.manager import Manager

from .conftest import GenNode, ConsumeNode, linux_run_only


def test_manager_logdir_string():
    manager = Manager(logdir=".", port=0)
    assert manager.logdir.parent == Path(".").resolve()
    manager.shutdown()


def test_manager_logdir_path():
    manager = Manager(logdir=Path("."), port=0)
    assert manager.logdir.parent == Path(".").resolve()
    manager.shutdown()
