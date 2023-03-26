import os
import pathlib
import shutil


def cleanup_and_recreate_dir(directory: pathlib.Path):
    """Cleanup a directory tree and recreate it."""
    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)
