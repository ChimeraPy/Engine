import os
import pickle
import logging
import datetime
from collections import deque
from typing import Dict, Optional, Any, List

import pandas as pd
from psutil import Process

from chimerapy.engine import config
from ..data_protocols import NodeDiagnostics
from ..async_timer import AsyncTimer
from ..networking.data_chunk import DataChunk
from ..service import Service
from ..eventbus import EventBus, TypedObserver, Event
from ..states import NodeState
from .events import NewOutBoundDataEvent, DiagnosticsReportEvent, EnableDiagnosticsEvent


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
        self._enable: bool = False
        self.process: Optional[Process] = None
        self.deques: Dict[str, deque[float]] = {
            "latency(ms)": deque(maxlen=config.get("diagnostics.deque-length")),
            "payload_size(KB)": deque(maxlen=config.get("diagnostics.deque-length")),
        }
        self.seen_uuids: deque[str] = deque(
            maxlen=config.get("diagnostics.deque-length")
        )
        self.async_timer = AsyncTimer(
            self.diagnostics_report, config.get("diagnostics.interval")
        )

        if self.state.logdir:
            self.log_file = self.state.logdir / "diagnostics.csv"
        else:
            raise RuntimeError(f"{self}: logdir {self.state.logdir} not set!")

        # Add observers to profile
        self.observers: Dict[str, TypedObserver] = {
            "setup": TypedObserver("setup", on_asend=self.setup, handle_event="drop"),
            "enable_diagnostics": TypedObserver(
                "enable_diagnostics",
                EnableDiagnosticsEvent,
                on_asend=self.enable,
                handle_event="unpack",
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

        # self.logger.debug(f"{self}: log_file={self.log_file}")

    async def enable(self, enable: bool = True):

        if enable != self._enable:

            if enable:
                # self.logger.debug(f"{self}: enabled")
                assert self.eventbus.thread

                # Add a timer function
                await self.async_timer.start()

                # Add observer
                observer = TypedObserver(
                    "out_step",
                    NewOutBoundDataEvent,
                    on_asend=self.post_step,
                    handle_event="unpack",
                )
                self.observers["out_step"] = observer
                await self.eventbus.asubscribe(observer)

            else:
                # self.logger.debug(f"{self}: disabled")
                # Stop the timer and remove the observer
                await self.async_timer.stop()

                observer = self.observers["enable_diagnostics"]
                await self.eventbus.aunsubscribe(observer)

            # Update
            self._enable = enable

    def setup(self):
        self.process = Process(pid=os.getpid())

    async def diagnostics_report(self):

        if not self.process or not self._enable:
            return None

        # Get the timestamp
        timestamp = datetime.datetime.now().isoformat()

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

    async def post_step(self, data_chunk: DataChunk):
        # assert self.process
        if not self.process:
            return None

        # Computing the diagnostics metrics per step
        payload_size = 0.0

        # Containers to compute metrics
        payload_sizes: List[float] = []

        # Obtain the meta data of the data chunk
        meta = data_chunk.get("meta")["value"]
        self.seen_uuids.append(data_chunk._uuid)

        # Get the payload size (of all keys)
        total_size = 0.0
        for key in data_chunk.contains():
            payload = data_chunk.get(key)["value"]
            total_size += self.get_object_kilobytes(payload)
        payload_sizes.append(total_size)

        # After processing all data_chunk keys, get payload total
        payload_size = sum(payload_sizes)

        # Store results
        self.deques["latency(ms)"].append(meta["delta"])
        self.deques["payload_size(KB)"].append(payload_size)

    def get_object_kilobytes(self, payload: Any) -> float:
        return len(pickle.dumps(payload)) / 1024

    async def teardown(self):
        await self.async_timer.stop()
