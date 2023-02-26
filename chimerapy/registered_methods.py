from typing import Callable, Dict, Type
from dataclasses import dataclass, field

# Reference:
# https://stackoverflow.com/a/69339176/13231446
# https://stackoverflow.com/a/54316392/13231446


@dataclass
class RegisteredMethod:
    function: Callable
    blocking: bool = True
    reset: bool = False
    params: Dict[str, Type] = field(default=dict)


# Class decorator for methods, that appends the decorated method to a cls variable
class register:
    def __init__(self, fn: Callable, **kwargs):
        self.fn = fn
        self.kwargs = dict(kwargs)

    def __set_name__(self, owner: "Node", name: str):
        owner.registered_methods[name] = RegisteredMethod(
            function=self.fn, **self.kwargs
        )
        setattr(owner, name, self.fn)

    def __call__(self, *args, **kwargs):
        pass

    @classmethod
    def with_config(
        cls, blocking: bool = True, reset: bool = False, params: Dict[str, Type] = {}
    ):
        return lambda func: cls(func, blocking=blocking, reset=reset, params=params)
