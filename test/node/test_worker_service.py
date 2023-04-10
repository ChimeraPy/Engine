from chimerapy.node.worker_service import WorkerService
from ..conftest import GenNode, TEST_DATA_DIR


def test_inject_service(logreceiver):
    worker_service = WorkerService(
        name="worker",
        host="0.0.0.0",
        port=9000,
        worker_logdir=TEST_DATA_DIR,
        in_bound=[],
        in_bound_by_name=[],
        out_bound=[],
    )
    node = GenNode(name="Gen", debug_port=logreceiver.port)
    worker_service.inject(node)
