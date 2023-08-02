from typing import Dict, Callable, Literal

from dataclasses import dataclass, field
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class RegisteredMethod:
    name: str
    style: str = "concurrent"  # Literal['concurrent', 'blocking', 'reset']
    params: Dict[str, str] = field(default_factory=dict)


# Reference:
# https://stackoverflow.com/a/69339176/13231446
# https://stackoverflow.com/a/54316392/13231446
# Class decorator for methods, that appends the decorated method to a cls variable
class register:
    def __init__(self, fn: Callable, **kwargs):
        self.fn = fn
        self.kwargs = dict(kwargs)

    def __set_name__(self, owner, name: str):
        # owner.registered_methods: Dict[str, RegisteredMethod] = {}
        if not hasattr(owner, "registered_methods"):
            owner.registered_methods = {}

        owner.registered_methods[name] = RegisteredMethod(name=name, **self.kwargs)
        setattr(owner, name, self.fn)

    def __call__(self, *args, **kwargs):
        """Required to make class decorator work."""
        pass

    @classmethod
    def with_config(
        cls,
        params: Dict[str, str] = {},
        style: Literal["concurrent", "blocking", "reset"] = "concurrent",
    ):
        return lambda func: cls(func, params=params, style=style)
