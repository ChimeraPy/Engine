import os
import shutil
import traceback
import socket
import asyncio
import pathlib
from typing import Optional, Literal, Union, Tuple

import aiohttp
from zeroconf import ServiceBrowser, Zeroconf

from chimerapy.engine import config
from ..networking import Client
from chimerapy.engine import _logger
from .worker_service import WorkerService
from .zeroconf_listener import ZeroconfListener


class HttpClientService(WorkerService):
    def __init__(self, name: str):
        super().__init__(name=name)
        self.manager_ack: bool = False
        self.connected_to_manager: bool = False
        self.manager_host = "0.0.0.0"
        self.manager_port = -1
        self.manager_url = ""

    def get_address(self) -> Tuple[str, int]:
        return self.manager_host, self.manager_port

    async def shutdown(self) -> bool:
        success = True
        if self.connected_to_manager:
            try:
                success = await self.async_deregister()
            except Exception:
                self.worker.logger.warning(f"{self}: Failed to properly deregister")
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
                data=self.worker.state.to_json(),
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
                data=self.worker.state.to_json(),
                timeout=timeout,
            ) as resp:

                if resp.ok:

                    # Get JSON
                    data = await resp.json()

                    config.update_defaults(data.get("config", {}))
                    logs_push_info = data.get("logs_push_info", {})

                    if logs_push_info["enabled"]:
                        self.worker.logger.info(
                            f"{self}: enabling logs push to Manager"
                        )
                        for logging_entity in [
                            self.worker.logger,
                            self.worker.logreceiver,
                        ]:
                            handler = _logger.add_zmq_push_handler(
                                logging_entity,
                                logs_push_info["host"],
                                logs_push_info["port"],
                            )
                            if logging_entity is not self.worker.logger:
                                _logger.add_identifier_filter(
                                    handler, self.worker.state.id
                                )

                    # Tracking the state and location of the manager
                    self.connected_to_manager = True
                    self.manager_host = host
                    self.manager_port = port
                    self.manager_url = f"http://{host}:{port}"

                    self.worker.logger.info(
                        f"{self}: connection successful to Manager @ {host}:{port}."
                    )
                    return True

        return False

    async def _async_connect_via_zeroconf(
        self, timeout: Union[int, float] = config.get("worker.timeout.zeroconf-search")
    ) -> bool:

        # Create the Zeroconf instance and the listener
        zeroconf = Zeroconf()
        listener = ZeroconfListener(
            stop_service_name="chimerapy", logger=self.worker.logger
        )

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

        # self.worker.logger.debug(f"{self}: connected via zeroconf: {success}")

        return success

    async def _send_archive_locally(self, path: pathlib.Path) -> bool:
        self.worker.logger.debug(f"{self}: sending archive locally")

        # First rename and then move
        delay = 1
        miss_counter = 0
        timeout = 10
        while True:
            try:
                shutil.move(self.worker.tempfolder, path)
                break
            except shutil.Error:  # File already exists!
                break
            except Exception:
                self.worker.logger.error(traceback.format_exc())
                await asyncio.sleep(delay)
                miss_counter += 1
                if miss_counter * delay > timeout:
                    raise TimeoutError("Nodes haven't fully finishing saving!")

        old_folder_name = path / self.worker.tempfolder.name
        new_folder_name = path / f"{self.worker.name}-{self.worker.id}"
        os.rename(old_folder_name, new_folder_name)
        return True

    async def _send_archive_remotely(self, host: str, port: int) -> bool:

        self.worker.logger.debug(f"{self}: sending archive via network")

        # Else, send the archive data to the manager via network
        try:
            # Create a temporary HTTP client
            client = Client(self.worker.id, host=host, port=port)
            return await client._send_folder_async(
                self.worker.name, self.worker.tempfolder
            )
        except (TimeoutError, SystemError) as error:
            self.delete_temp = False
            self.worker.logger.exception(
                f"{self}: Failed to transmit files to Manager - {error}."
            )

        return False

    async def _async_node_status_update(self) -> bool:

        async with aiohttp.ClientSession(self.manager_url) as session:
            async with session.post(
                "/workers/node_status", data=self.worker.state.to_json()
            ) as resp:

                return resp.ok
