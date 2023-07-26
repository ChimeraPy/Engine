from typing import Dict, Any, Optional

from chimerapy.engine import config
from ..logger.distributed_logs_sink import DistributedLogsMultiplexedFileSink
from chimerapy.engine.utils import megabytes_to_bytes
from .manager_service import ManagerService
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine")


class DistributedLoggingService(ManagerService):
    def __init__(self, name: str, publish_logs_via_zmq: bool, **kwargs):
        super().__init__(name=name)

        # Save parameters
        self.name = name
        self.logs_sink: Optional[DistributedLogsMultiplexedFileSink] = None

        if publish_logs_via_zmq:
            handler_config = _logger.ZMQLogHandlerConfig.from_dict(kwargs)
            _logger.add_zmq_handler(logger, handler_config)

    def start(self):

        if config.get("manager.logs-sink.enabled"):
            self.logs_sink = self._start_logs_sink()
        else:
            self.logs_sink = None

    async def shutdown(self):

        # Stop the distributed logger
        if self.logs_sink:
            self.logs_sink.shutdown()
            logger.debug(f"{self}: SHUTTING DOWN LOGS SINK")

    #####################################################################################
    ## Helper Function
    #####################################################################################

    def register_entity(self, worker_name: str, worker_id: str):

        if self.logs_sink is not None:
            self._register_worker_to_logs_sink(
                worker_name=worker_name, worker_id=worker_id
            )

    def deregister_entity(self, worker_id: str):

        if self.logs_sink is not None:
            self.logs_sink.deregister_entity(worker_id)

    def get_log_info(self) -> Dict[str, Any]:

        # Extract information
        ip = None
        port = None
        if self.logs_sink:
            port = self.logs_sink.port
            ip = self.services.http_server.ip

        return {"enabled": self.logs_sink is not None, "host": ip, "port": port}

    def _register_worker_to_logs_sink(self, worker_name: str, worker_id: str):
        if not self.services.session_record.logdir.exists():
            self.services.session_record.logdir.mkdir(parents=True)
        if self.logs_sink:
            self.logs_sink.initialize_entity(
                worker_name, worker_id, self.services.session_record.logdir
            )
            logger.info(f"Registered worker {worker_name} to logs sink")

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
