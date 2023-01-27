# Built-in Imports
from typing import Dict, Any
import pathlib
import os

# Third-party Imports
import yaml

config_file = pathlib.Path(os.path.abspath(__file__)).parent / "chimerapyrc.yaml"

with open(config_file) as f:
    defaults = yaml.safe_load(f)["config"]

config: Dict[str, Any] = {}


def update(new: Dict[str, Any]):
    config.update(new)


def get(key):
    keys = key.split(".")
    result = config
    for k in keys:
        result = result[k]

    return result


update(defaults)
