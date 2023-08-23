import os
import pickle
import logging
from collections import deque
from typing import Dict, Optional, Any, List

import pandas as pd
from psutil import Process

from chimerapy.engine import config
from chimerapy.engine import clock
from ..data_protocols import NodeDiagnostics
from ..async_timer import AsyncTimer
from ..networking.data_chunk import DataChunk
from ..service import Service
from ..eventbus import EventBus, TypedObserver, Event
from ..states import NodeState
from .events import NewInBoundDataEvent, DiagnosticsReportEvent


class ProfilerService(Service):
    def __init__(
        self, name: str, state: NodeState, eventbus: EventBus, logger: logging.Logger
    ):
        super().__init__(name=name)

        # Save parameters
        self.state = state
        self.eventbus = eventbus
        self.logger = logger

        # State variables
        self.process: Optional[Process] = None
        self.deques: Dict[str, deque[float]] = {
            "latency(ms)": deque(maxlen=config.get("diagnostics.deque-length")),
            "payload_size(KB)": deque(maxlen=config.get("diagnostics.deque-length")),
        }
        self.seen_uuids: deque[str] = deque(
            maxlen=config.get("diagnostics.deque-length")
        )

        if self.state.logdir:
            self.log_file = self.state.logdir / "diagnostics.csv"
        else:
            raise RuntimeError(f"{self}: logdir {self.state.logdir} not set!")

        # Add a timer function
        self.async_timer = AsyncTimer(
            self.diagnostics_report, config.get("diagnostics.interval")
        )
        assert self.eventbus.thread
        self.eventbus.thread.exec(self.async_timer.start()).result()

        # Add observers to profile
        self.observers: Dict[str, TypedObserver] = {
            "setup": TypedObserver("setup", on_asend=self.setup, handle_event="drop"),
            "in_step": TypedObserver(
                "in_step",
                NewInBoundDataEvent,
                on_asend=self.pre_step,
                handle_event="unpack",
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

        # self.logger.debug(f"{self}: log_file={self.log_file}")

    def setup(self):
        # self.logger.debug(f"{self}: setup")
        self.process = Process(pid=os.getpid())

    async def diagnostics_report(self):

        if not self.process:
            return None

        # Get the timestamp
        timestamp = clock.now().isoformat()

        # Get process-wide information
        memory = self.process.memory_info()
        memory_usage = memory.rss / 1024
        cpu_usage = self.process.cpu_percent()

        # Compute interval information of latency and payload size
        num_of_steps = len(self.deques["latency(ms)"])
        if num_of_steps:
            # Compute values
            mean_latency = sum(self.deques["latency(ms)"]) / num_of_steps
            total_payload = sum(self.deques["payload_size(KB)"])

            # Clear queues
            for _deque in self.deques.values():
                _deque.clear()

        else:
            mean_latency = 0
            total_payload = 0

        # Save information then
        diag = NodeDiagnostics(
            timestamp=timestamp,
            latency=mean_latency,
            payload_size=total_payload,
            memory_usage=memory_usage,
            cpu_usage=cpu_usage,
            num_of_steps=num_of_steps,
        )

        # Send the information to the Worker and ultimately the Manager
        event_data = DiagnosticsReportEvent(diag)
        # self.logger.debug(f"{self}: data = {diag}")
        await self.eventbus.asend(Event("diagnostics_report", event_data))

        # Write to a csv, if diagnostics enabled
        if config.get("diagnostics.logging-enabled"):

            # Create dictionary with units
            data = {
                "timestamp": timestamp,
                "latency(ms)": mean_latency,
                "payload_size(KB)": total_payload,
                "memory_usage(KB)": memory_usage,
                "cpu_usage(%)": cpu_usage,
                "num_of_steps(int)": num_of_steps,
            }

            df = pd.Series(data).to_frame().T

            df.to_csv(
                str(self.log_file),
                mode="a",
                header=not self.log_file.exists(),
                index=False,
            )

    async def pre_step(self, data_chunks: Dict[str, DataChunk]):
        # assert self.process
        if not self.process:
            return None

        # Computing the diagnostics metrics per step (multiple datachunks)
        mean_latency = 0.0
        payload_size = 0.0

        # Containers to compute metrics
        latencies: List[float] = []
        payload_sizes: List[float] = []

        # First compute latency
        for name, chunk in data_chunks.items():

            # Only process unseen data_chunks, as in the "in_step" event,
            # it is possible for the same data chunks to be re-used in the step
            if chunk.uuid in self.seen_uuids:
                continue
            else:
                self.seen_uuids.append(chunk.uuid)

            # Obtain the meta data of the data chunk
            meta = chunk.get("meta")["value"]

            # Get latency
            latency = (meta["received"] - meta["transmitted"]).total_seconds() * 1000
            latencies.append(latency)

            # Get the payload size (of all keys)
            total_size = 0.0
            for key in chunk.contains():
                payload = chunk.get(key)["value"]
                total_size += self.get_object_kilobytes(payload)
            payload_sizes.append(total_size)

        # After processing all data_chunks, get latency and payload means
        mean_latency = sum(latencies) / len(latencies)
        payload_size = sum(payload_sizes)

        # Store results
        self.deques["latency(ms)"].append(mean_latency)
        self.deques["payload_size(KB)"].append(payload_size)

    def get_object_kilobytes(self, payload: Any) -> float:
        # return sys.getsizeof(payload) / 1024
        return len(pickle.dumps(payload))

    async def teardown(self):
        await self.async_timer.stop()
