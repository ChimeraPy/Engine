import typing
from typing import List, Optional, Literal, Union, Tuple

if typing.TYPE_CHECKING:
    from .node import Node

import dill


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
        node: Optional[Union["Node", Tuple[str, bytes]]] = None,
        in_bound: Optional[List[str]] = None,
        in_bound_by_name: Optional[List[str]] = None,
        out_bound: Optional[List[str]] = None,
        follow: Optional[str] = None,
        context: Literal["multiprocessing", "threading"] = "multiprocessing",
    ):

        # Save parameters
        if in_bound is None:
            in_bound = []
        if in_bound_by_name is None:
            in_bound_by_name = []
        if out_bound is None:
            out_bound = []

        self.in_bound = in_bound
        self.in_bound_by_name = in_bound_by_name
        self.out_bound = out_bound
        self.follow = follow
        self.context = context

        if node:
            if isinstance(node, tuple):
                self.id = node[0]
                self.pickled = node[1]
            else:
                self.id = node.id
                self.pickled = dill.dumps(node, recurse=True)
        else:
            self.id = ""
            self.pickled = bytes([])

    def __str__(self):
        string = (
            f"<{self.__class__.__name__}, id={self.id} "
            f"in_bound={self.in_bound} "
            f"in_bound_by_name={self.in_bound_by_name} "
            f"out_bound={self.out_bound} "
            f"follow={self.follow} "
            f"context={self.context}>"
        )

        return string
