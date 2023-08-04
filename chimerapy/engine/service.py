from typing import Dict, List, Any, Optional
from collections import UserDict


class Service:
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return f"<{self.__class__.__name__}, name={self.name}>"

    def shutdown(self):
        ...


class ServiceGroup(UserDict):

    data: Dict[str, Service]

    def apply(self, method_name: str, order: Optional[List[str]] = None):

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

    async def async_apply(
        self, method_name: str, order: Optional[List[str]] = None
    ) -> List[Any]:

        outputs: List[Any] = []
        if order:
            for s_name in order:
                if s_name in self.data:
                    s = self.data[s_name]
                    func = getattr(s, method_name)
                    outputs.append(await func())
        else:
            for s in self.data.values():
                func = getattr(s, method_name)
                outputs.append(await func())

        return outputs
