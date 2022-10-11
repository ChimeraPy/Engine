from typing import Union


def create_and_connect_virtual_client(
    machine_name: str, host: str, port: Union[str, int]
):

    if isinstance(port, str):
        port = int(port)

    ...
