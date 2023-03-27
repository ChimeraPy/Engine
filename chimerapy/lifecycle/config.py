from pathlib import Path
from itertools import chain
import json


def read_config():
    """Read the configuration file."""

    class TRANSITIONS:
        def __init__(self):
            raise ValueError("Cannot instantiate this class.")

    with open(Path(__file__).parent / "lifecycle.json") as f:
        config = json.load(f)
    transitions = chain(
        *map(lambda x: x["allowed_transitions"].keys(), config["states"].values())
    )
    for transition in transitions:
        setattr(TRANSITIONS, transition, transition)

    return config, TRANSITIONS
