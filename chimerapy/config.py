# Built-in Imports
from typing import Dict, Any
import pathlib
import os

# Third-party Imports
import yaml

# Required to be string!
config_file = str(pathlib.Path(os.path.abspath(__file__)).parent / "chimerapyrc.yaml")

with open(config_file) as f:
    defaults = yaml.safe_load(f)["config"]

del f

config: Dict[str, Any] = {}


def update_defaults(new: Dict[str, Any]):
    """Updated key value pairs from a dictionary.

    Args:
        new (Dict[str, Any]): The new updated kv pairs
    """
    config.update(new)


def get(key: str) -> Any:
    """Get the configuration

    Args:
        key (str): The requested key

    Returns:
        Any: Value of the key

    """
    keys = key.split(".")
    result = config
    for k in keys:
        result = result[k]

    return result


def set(key: str, value: Any):
    """Set the configuration

    Args:
        key (str): The requested key
        value (Any): The value to set
    """
    keys = key.split(".")
    result = config
    for k in keys[:-1]:
        result = result[k]

    result[keys[-1]] = value


# Load the defaults
update_defaults(defaults)
