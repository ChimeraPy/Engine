from typing import Dict, Optional

from aiodistbus import registry

from chimerapy.engine import _logger, config

from ..logger.distributed_logs_sink import DistributedLogsMultiplexedFileSink
from ..service import Service
from ..states import ManagerState, WorkerState
from ..utils import megabytes_to_bytes

logger = _logger.getLogger("chimerapy-engine")


class DistributedLoggingService(Service):
    def __init__(
        self,
        name: str,
        publish_logs_via_zmq: bool,
        state: ManagerState,
        **kwargs,
    ):
        super().__init__(name=name)

        # Save parameters
        self.name = name
        self.logs_sink: Optional[DistributedLogsMultiplexedFileSink] = None
        self.state = state

        if publish_logs_via_zmq:
            handler_config = _logger.ZMQLogHandlerConfig.from_dict(kwargs)
            _logger.add_zmq_handler(logger, handler_config)

    @registry.on("start", namespace=f"{__name__}.DistributedLoggingService")
    def start(self):

        if config.get("manager.logs-sink.enabled"):
            self.logs_sink = self._start_logs_sink()
            self.state.log_sink_enabled = True
        else:
            self.logs_sink = None

    @registry.on("shutdown", namespace=f"{__name__}.DistributedLoggingService")
    def shutdown(self):

        if self.logs_sink:
            self.logs_sink.shutdown()

    #####################################################################################
    ## Helper Function
    #####################################################################################

    @registry.on(
        "entity_register",
        WorkerState,
        namespace=f"{__name__}.DistributedLoggingService",
    )
    def register_entity(self, state: WorkerState):

        # logger.debug(f"{self}: registereing entity: {worker_name}, {worker_id}")
        id, name = state.id, state.name

        if self.logs_sink is not None:
            self._register_worker_to_logs_sink(worker_name=name, worker_id=id)

    @registry.on(
        "entity_deregister", str, namespace=f"{__name__}.DistributedLoggingService"
    )
    def deregister_entity(self, worker_id: str):

        if self.logs_sink is not None:
            self.logs_sink.deregister_entity(worker_id)

    def _register_worker_to_logs_sink(self, worker_name: str, worker_id: str):
        if self.logs_sink:
            self.logs_sink.initialize_entity(worker_name, worker_id, self.state.logdir)
            # logger.info(f"Registered worker {worker_name} to logs sink")

    @staticmethod
    def _start_logs_sink() -> DistributedLogsMultiplexedFileSink:
        """Start the logs sink."""
        max_bytes_per_worker = megabytes_to_bytes(
            config.get("manager.logs-sink.max-file-size-per-worker")
        )
        logs_sink = _logger.get_distributed_logs_multiplexed_file_sink(
            max_bytes=max_bytes_per_worker
        )
        logs_sink.start(register_exit_handlers=True)
        return logs_sink
