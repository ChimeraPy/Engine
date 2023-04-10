from typing import Type, Union, Dict, List
from collections import UserDict


class Service:
    def __init__(self, name: str):
        self.name = name

    def shutdown(self):
        ...


class ServiceGroup(UserDict):

    data: Dict[str, Service]

    def apply(self, method_name: str, order: List[str] = []):

        if order:
            for s_name in order:
                if s_name in self.data:
                    s = self.data[s_name]
                    func = getattr(s, method_name)
                    func()
        else:
            for s in self.data.values():
                func = getattr(s, method_name)
                func()
