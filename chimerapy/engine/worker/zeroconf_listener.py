import time
import logging
from datetime import datetime, timedelta

from zeroconf import ServiceInfo, ServiceListener
import zeroconf


class ZeroconfListener(ServiceListener):
    def __init__(
        self,
        stop_service_name: str = None,
        logger: logging.Logger = None,
        wait_time: float = 5.0,
    ):
        # Saving input parameters
        self.stop_service_name = stop_service_name

        if not logger:
            self.logger = logging.getLogger("chimerapy-engine")
        else:
            self.logger = logger

        # Containers
        self.is_service_found = False
        self.service_info: zeroconf._services.info.ServiceInfo = {}
        self.wait_time = wait_time

    def update_service(self, *args, **kwargs):
        """Mandatory method, but can be empty"""
        ...

    def remove_service(self, *args, **kwargs):
        """Mandatory method, but can be empty"""
        ...

    def add_service(self, zeroconf, type, name):
        """Add detected services and stop if ``chimerapy-engine`` detected!"""
        info = zeroconf.get_service_info(type, name)

        if isinstance(info, ServiceInfo):
            if self.stop_service_name and name.startswith(self.stop_service_name):

                self.logger.info(f"chimerapy-engine zeroconf service detected: {info}")

                # Wait for wait_time seconds to confirm if this is the latest version
                end_time = datetime.now() + timedelta(seconds=self.wait_time)
                while datetime.now() < end_time:

                    # Wait a little bit
                    time.sleep(1)

                    # Get the latest info
                    try:
                        latest_info = zeroconf.get_service_info(type, name)
                    except Exception:
                        continue

                    if b"timestamp" not in info.properties:
                        info = latest_info
                    elif b"timestamp" not in latest_info.properties:
                        continue

                    # Convert time stamps to datetime objects
                    info_tstamp = datetime.strptime(
                        info.properties[b"timestamp"].decode(), "%Y_%m_%d_%H_%M_%S"
                    )
                    l_info_tstamp = datetime.strptime(
                        latest_info.properties[b"timestamp"].decode(),
                        "%Y_%m_%d_%H_%M_%S",
                    )

                    # If latest info timestamp is more recent, use the latests
                    if l_info_tstamp == max(l_info_tstamp, info_tstamp):
                        info = latest_info
                        break

                self.is_service_found = True
                self.service_info = info
