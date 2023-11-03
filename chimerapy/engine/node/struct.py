import logging
from dataclasses import dataclass
from typing import Any, Dict

from dataclasses_json import DataClassJsonMixin

from ..networking.client import Client
from ..states import NodeState


@dataclass
class PreSetupData(DataClassJsonMixin):
    state: NodeState
    logger: logging.Logger


@dataclass
class RegisteredMethodData(DataClassJsonMixin):
    method_name: str
    params: Dict[str, Any]
    client: Client
