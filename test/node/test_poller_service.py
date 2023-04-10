from chimerapy.node.poller_service import PollerService
from ..conftest import GenNode


def test_poller_service(logreceiver):
    node = GenNode(name="Gen", debug_port=logreceiver.port)
    service = PollerService(
        name="poller", in_bound=["a"], in_bound_by_name=["a"], follow="a"
    )
    service.inject(node)
    msg = {"data": {"a": {"host": "172.0.0.1", "port": "12345"}}}

    service.setup_connections(msg)

    node.shutdown()
    service.teardown()
