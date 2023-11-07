import datetime
import logging
import os
import pickle
from collections import deque
from typing import Any, Dict, List, Optional

import pandas as pd
from aiodistbus import registry
from psutil import Process

from chimerapy.engine import config

from ..async_timer import AsyncTimer
from ..data_protocols import NodeDiagnostics
from ..networking.data_chunk import DataChunk
from ..service import Service
from ..states import NodeState


class ProfilerService(Service):
    def __init__(self, name: str, state: NodeState, logger: logging.Logger):
        super().__init__(name=name)

        # Save parameters
        self.state = state
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

    @registry.on("worker.diagnostics", bool, namespace=f"{__name__}.ProfilerService")
    async def enable(self, enable: bool = True):
        # self.logger.debug(f"Profiling enabled: {enable}")

        if enable != self._enable:

            if enable:
                # Add a timer function
                await self.async_timer.start()
                await self.entrypoint.on("out_step", self.post_step, DataChunk)

            else:
                # Stop the timer and remove the observer
                await self.async_timer.stop()
                await self.entrypoint.off("out_step")

            # Update
            self._enable = enable

    @registry.on("setup", namespace=f"{__name__}.ProfilerService")
    async def setup(self):

        # Construct the path
        if self.state.logdir:
            self.log_file = self.state.logdir / "diagnostics.csv"
        else:
            raise RuntimeError(f"{self}: logdir {self.state.logdir} not set!")

        self.process = Process(pid=os.getpid())

    async def diagnostics_report(self):

        if not self.process or not self._enable:
            return None

        # self.logger.debug(f"{self}: Collecting diagnostics...")

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
            node_id=self.state.id,
            timestamp=timestamp,
            latency=mean_latency,
            payload_size=total_payload,
            memory_usage=memory_usage,
            cpu_usage=cpu_usage,
            num_of_steps=num_of_steps,
        )

        # Send the information to the Worker and ultimately the Manager
        # await self.entrypoint.emit("node.diagnostics_report", diag)
        self.state.diagnostics = diag

        # self.logger.debug(f"{self}: collected diagnostics")

        # Write to a csv, if diagnostics enabled
        if config.get("diagnostics.logging-enabled"):

            # self.logger.debug(f"{self}: Saving data")

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

        # self.logger.debug(f"{self}: Received data chunk {data_chunk._uuid}.")

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

    @registry.on("teardown", namespace=f"{__name__}.ProfilerService")
    async def teardown(self):
        await self.async_timer.stop()
