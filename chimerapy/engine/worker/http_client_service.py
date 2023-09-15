import os
import shutil
import json
import uuid
import traceback
import socket
import asyncio
import pathlib
import logging
from typing import Optional, Literal, Union, Tuple, Dict

import aiohttp
from zeroconf import ServiceBrowser, Zeroconf

from chimerapy.engine import config
from chimerapy.engine import _logger
from ..logger.zmq_handlers import NodeIDZMQPullListener
from ..states import WorkerState
from ..networking import Client
from ..utils import get_ip_address
from ..service import Service
from ..eventbus import EventBus, TypedObserver
from .events import SendArchiveEvent
from .zeroconf_listener import ZeroconfListener


class HttpClientService(Service):
    def __init__(
        self,
        name: str,
        state: WorkerState,
        eventbus: EventBus,
        logger: logging.Logger,
        logreceiver: NodeIDZMQPullListener,
    ):
        super().__init__(name=name)

        # Input parameters
        self.state = state
        self.eventbus = eventbus
        self.logger = logger
        self.logreceiver = logreceiver

        # Containers
        self.manager_ack: bool = False
        self.connected_to_manager: bool = False
        self.manager_host = "0.0.0.0"
        self.manager_port = -1
        self.manager_url = ""

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
            "WorkerState.changed": TypedObserver(
                "WorkerState.changed",
                on_asend=self._async_node_status_update,
                handle_event="drop",
            ),
            "send_archive": TypedObserver(
                "send_archive",
                SendArchiveEvent,
                on_asend=self._send_archive,
                handle_event="unpack",
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    def get_address(self) -> Tuple[str, int]:
        return self.manager_host, self.manager_port

    async def shutdown(self) -> bool:
        success = True
        if self.connected_to_manager:
            try:
                success = await self.async_deregister()
            except Exception:
                # self.logger.warning(f"{self}: Failed to properly deregister")
                success = False

        return success

    ###################################################################################
    ## Client Methods
    ###################################################################################

    async def async_connect(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        method: Optional[Literal["ip", "zeroconf"]] = "ip",
        timeout: Union[int, float] = config.get("worker.timeout.info-request"),
    ) -> bool:
        """Connect ``Worker`` to ``Manager``.

        This establish server-client connections between ``Worker`` and
        ``Manager``. To ensure that the connections are close correctly,
        either the ``Manager`` or ``Worker`` should shutdown before
        stopping your program to avoid processes and threads that do
        not shutdown.

        Args:
            method (Literal['ip', 'zeroconf']): The approach to connecting to \
                ``Manager``
            host (str): The ``Manager``'s IP address.
            port (int): The ``Manager``'s port number
            timeout (Union[int, float]): Set timeout for the connection.

        Returns:
            Future[bool]: Success in connecting to the Manager

        """

        # First check if we are already connected
        if self.connected_to_manager:
            self.logger.warning(f"{self}: Requested to connect when already connected")
            return True

        # Standard direct IP connection
        if method == "ip":
            if not isinstance(host, str) or not isinstance(port, int):
                raise RuntimeError(
                    f"Invalid mode ``{method}`` with missing parameters: host={host}, \
                    port={port}"
                )
            return await self._async_connect_via_ip(host, port, timeout)

        # Using Zeroconf
        elif method == "zeroconf":
            success = await self._async_connect_via_zeroconf(timeout)
            return success

        # Invalid option
        else:
            raise RuntimeError(f"Invalid connect type: {method}")

    async def async_deregister(self) -> bool:

        # Send the request to each worker
        async with aiohttp.ClientSession() as client:
            async with client.post(
                self.manager_url + "/workers/deregister",
                data=self.state.to_json(),
                timeout=config.get("worker.timeout.deregister"),
            ) as resp:

                if resp.ok:
                    self.connected_to_manager = False

                return resp.ok

        return False

    async def _async_connect_via_ip(
        self,
        host: str,
        port: int,
        timeout: Union[int, float] = config.get("worker.timeout.info-request"),
    ) -> bool:

        # Send the request to each worker
        async with aiohttp.ClientSession() as client:
            async with client.post(
                f"http://{host}:{port}/workers/register",
                data=self.state.to_json(),
                timeout=timeout,
            ) as resp:

                if resp.ok:

                    # Get JSON
                    data = await resp.json()

                    config.update_defaults(data.get("config", {}))
                    logs_push_info = data.get("logs_push_info", {})

                    if logs_push_info["enabled"]:
                        # self.logger.info(
                        #     f"{self}: enabling logs push to Manager: {logs_push_info}"
                        # )
                        for logging_entity in [
                            self.logger,
                            self.logreceiver,
                        ]:
                            handler = _logger.add_zmq_push_handler(
                                logging_entity,
                                logs_push_info["host"],
                                logs_push_info["port"],
                            )
                            if logging_entity is not self.logger:
                                _logger.add_identifier_filter(handler, self.state.id)

                    # Tracking the state and location of the manager
                    self.connected_to_manager = True
                    self.manager_host = host
                    self.manager_port = port
                    self.manager_url = f"http://{host}:{port}"

                    self.logger.info(
                        f"{self}: connection successful to Manager @ {host}:{port}."
                    )
                    return True

        return False

    async def _async_connect_via_zeroconf(
        self, timeout: Union[int, float] = config.get("worker.timeout.zeroconf-search")
    ) -> bool:

        # Create the Zeroconf instance and the listener
        zeroconf = Zeroconf()
        listener = ZeroconfListener(stop_service_name="chimerapy", logger=self.logger)

        # Browse for services
        browser = ServiceBrowser(zeroconf, "_http._tcp.local.", listener)

        # Wait for a certain service to be found or 10 seconds, whichever comes first
        delay = 0.1
        miss = 0
        success = False
        while True:

            # Check if ChimeraPy service was found
            if listener.is_service_found:

                host = socket.inet_ntoa(listener.service_info.addresses[0])
                port = listener.service_info.port

                # Connect
                success = await self._async_connect_via_ip(
                    host=host, port=port, timeout=timeout
                )
                break

            # If not, wait
            await asyncio.sleep(delay)
            miss += 1

            if (miss + 1) * delay > timeout:
                break

        # Clean up
        browser.cancel()
        zeroconf.close()

        # self.logger.debug(f"{self}: connected via zeroconf: {success}")

        return success

    async def _send_archive(self, path: pathlib.Path) -> bool:

        # Flag
        send_locally = False

        # Get manager info
        manager_host, manager_port = self.get_address()

        if not self.connected_to_manager:
            send_locally = True
        elif manager_host == get_ip_address():
            send_locally = True
        else:
            send_locally = False

        success = False
        try:
            if send_locally:
                await self._send_archive_locally(path)
            else:
                await self._send_archive_remotely(manager_host, manager_port)
            success = True
        except Exception:
            self.logger.error(traceback.format_exc())

        # Send information to manager about success
        if self.connected_to_manager:
            data = {"worker_id": self.state.id, "success": success}
            async with aiohttp.ClientSession(self.manager_url) as session:
                async with session.post(
                    "/workers/send_archive", data=json.dumps(data)
                ) as _:
                    ...
                    # self.logger.debug(f"{self}: send "
                    # "archive update confirmation: {resp.ok}")

        return success

    async def _send_archive_locally(self, path: pathlib.Path) -> pathlib.Path:
        self.logger.debug(f"{self}: sending archive locally")

        # First rename and then move
        delay = 1
        miss_counter = 0
        timeout = 10
        while True:
            try:
                shutil.move(self.state.tempfolder, path)
                break
            except shutil.Error:  # File already exists!
                break
            except Exception:
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(delay)
                miss_counter += 1
                if miss_counter * delay > timeout:
                    raise TimeoutError("Nodes haven't fully finishing saving!")

        old_folder_name = path / self.state.tempfolder.name
        new_folder_name = (
            path / f"{self.state.name}-{self.state.id}-{str(uuid.uuid4())[:4]}"
        )
        os.rename(old_folder_name, new_folder_name)
        return new_folder_name

    async def _send_archive_remotely(self, host: str, port: int) -> bool:

        self.logger.debug(f"{self}: sending archive via network")

        # Else, send the archive data to the manager via network
        try:
            # Create a temporary HTTP client
            client = Client(self.state.id, host=host, port=port)
            return await client.async_send_folder(
                self.state.name, self.state.tempfolder
            )
        except (TimeoutError, SystemError) as error:
            self.delete_temp = False
            self.logger.exception(
                f"{self}: Failed to transmit files to Manager - {error}."
            )

        return False

    async def _async_node_status_update(self) -> bool:

        # self.logger.debug(f"{self}: WorkerState update: {self.state}")

        if not self.connected_to_manager:
            return False

        try:
            async with aiohttp.ClientSession(self.manager_url) as session:
                async with session.post(
                    "/workers/node_status", data=self.state.to_json()
                ) as resp:

                    return resp.ok
        except aiohttp.client_exceptions.ClientOSError:
            return False
