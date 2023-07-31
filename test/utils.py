import os
import pathlib
import shutil
from uuid import uuid4 as v4


def cleanup_and_recreate_dir(directory: pathlib.Path):
    """Cleanup a directory tree and recreate it."""
    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)


def uuid() -> str:
    """Generate a UUID."""
    return str(v4())


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__  # type: ignore[assignment]
    __delattr__ = dict.__delitem__  # type: ignore[assignment]
