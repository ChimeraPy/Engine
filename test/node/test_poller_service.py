from chimerapy.node.poller_service import PollerService
from ..conftest import GenNode


def test_poller_service(logreceiver):
    node = GenNode(name="Gen", debug_port=logreceiver.port)
    service = PollerService(
        name="poller",
        in_bound=["3c767221-f173-4bae-829f-cf018053008c"],
        in_bound_by_name=["3c767221-f173-4bae-829f-cf018053008c"],
        follow="3c767221-f173-4bae-829f-cf018053008c",
    )
    service.inject(node)
    msg = {
        "signal": 21,
        "timestamp": "0:00:00",
        "data": {
            "3c767221-f173-4bae-829f-cf018053008c": {
                "host": "10.76.221.127",
                "port": 0,
            },
            "a50ab1e1-eaa7-49d5-bd84-70c3d8bdbfcf": {
                "host": "10.76.221.127",
                "port": 45917,
            },
        },
        "uuid": "95c08c03-b267-41b7-945c-02e911e82ec3",
        "ok": False,
    }

    service.setup_connections(msg)

    node.shutdown()
    service.teardown()
