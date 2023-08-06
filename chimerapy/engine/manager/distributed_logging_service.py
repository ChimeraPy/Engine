from typing import Dict, Optional

from chimerapy.engine import config
from chimerapy.engine import _logger
from ..utils import megabytes_to_bytes
from ..eventbus import EventBus, TypedObserver
from ..service import Service
from ..states import ManagerState
from ..logger.distributed_logs_sink import DistributedLogsMultiplexedFileSink
from .events import RegisterEntityEvent, DeregisterEntityEvent

logger = _logger.getLogger("chimerapy-engine")


class DistributedLoggingService(Service):
    def __init__(
        self,
        name: str,
        publish_logs_via_zmq: bool,
        eventbus: EventBus,
        state: ManagerState,
        **kwargs,
    ):
        super().__init__(name=name)

        # Save parameters
        self.name = name
        self.logs_sink: Optional[DistributedLogsMultiplexedFileSink] = None
        self.eventbus = eventbus
        self.state = state

        if publish_logs_via_zmq:
            handler_config = _logger.ZMQLogHandlerConfig.from_dict(kwargs)
            _logger.add_zmq_handler(logger, handler_config)

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "start": TypedObserver("start", on_asend=self.start, handle_event="drop"),
            "entity_register": TypedObserver(
                "entity_register",
                on_asend=self.register_entity,
                event_data_cls=RegisterEntityEvent,
                handle_event="unpack",
            ),
            "entity_deregister": TypedObserver(
                "entity_deregister",
                on_asend=self.deregister_entity,
                event_data_cls=DeregisterEntityEvent,
                handle_event="unpack",
            ),
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    def start(self):

        if config.get("manager.logs-sink.enabled"):
            self.logs_sink = self._start_logs_sink()
            self.state.log_sink_enabled = True
        else:
            self.logs_sink = None

    def shutdown(self):

        if self.logs_sink:
            self.logs_sink.shutdown()

    #####################################################################################
    ## Helper Function
    #####################################################################################

    def register_entity(self, worker_name: str, worker_id: str):

        # logger.debug(f"{self}: registereing entity: {worker_name}, {worker_id}")

        if self.logs_sink is not None:
            self._register_worker_to_logs_sink(
                worker_name=worker_name, worker_id=worker_id
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
