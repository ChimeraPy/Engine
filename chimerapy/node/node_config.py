import typing
from typing import List, Optional, Literal

import dill

if typing.TYPE_CHECKING:
    from .node import Node


class NodeConfig:
    id: str
    pickled: bytes
    in_bound: List[str]
    in_bound_by_name: List[str]
    out_bound: List[str]
    follow: Optional[str]
    context: Literal["multiprocessing", "threading"]

    def __init__(
        self,
        node: "Node",
        in_bound: List[str] = [],
        in_bound_by_name: List[str] = [],
        out_bound: List[str] = [],
        follow: Optional[str] = None,
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
    ):

        # Save parameters
        self.in_bound = in_bound
        self.in_bound_by_name = in_bound_by_name
        self.out_bound = out_bound
        self.follow = follow
        self.context = context

        self.id = node.id
        self.pickled = dill.dumps(node, recurse=True)
