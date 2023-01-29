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


# Load the defaults
update_defaults(defaults)
