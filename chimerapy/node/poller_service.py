from typing import Optional, Dict, Tuple, List
import threading

import zmq

from ..networking import Subscriber, DataChunk
from .node_service import NodeService


class PollerService(NodeService):
    def __init__(
        self,
        name: str,
        in_bound: List[str],
        in_bound_by_name: List[str],
        follow: Optional[str],
    ):
        super().__init__(name)

        # Parameters
        self.in_bound: List[str] = in_bound
        self.in_bound_by_name: List[str] = in_bound_by_name
        self.follow: Optional[str] = follow

        # Containers
        self.p2p_subs: Dict[str, Subscriber] = {}
        self.socket_to_sub_name_mapping: Dict[zmq.Socket, Tuple[str, str]] = {}
        self.sub_poller = zmq.Poller()
        self.poll_inputs_thread: Optional[threading.Thread] = None

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def setup(self):

        # # Creating container for the latest values of the subscribers
        self.in_bound_data: Dict[str, Optional[DataChunk]] = {
            x: None for x in self.in_bound_by_name
        }
        self.inputs_ready = threading.Event()
        self.inputs_ready.clear()

    def teardown(self):

        # Stop poller
        if self.poll_inputs_thread:
            self.poll_inputs_thread.join()
            self.node.logger.debug(f"{self}: polling thread shutdown")

        # Shutting down subscriber
        for sub in self.p2p_subs.values():
            sub.shutdown()
            self.node.logger.debug(f"{self}: subscriber shutdown")

        self.node.logger.debug(f"{self.node}-PollerService shutdown")

    ####################################################################
    ## Helper Methods
    ####################################################################

    def setup_connections(self, msg: Dict):

        # We determine all the out bound nodes
        for i, in_bound_id in enumerate(self.in_bound):

            self.node.logger.debug(
                f"{self}: Setting up clients: {self.node.state.id}: {msg}"
            )

            # Determine the host and port information
            in_bound_info = msg["data"][in_bound_id]

            # Create subscribers to other nodes' publishers
            p2p_subscriber = Subscriber(
                host=in_bound_info["host"], port=in_bound_info["port"]
            )

            # Storing all subscribers
            self.p2p_subs[in_bound_id] = p2p_subscriber
            self.socket_to_sub_name_mapping[p2p_subscriber._zmq_socket] = (
                self.in_bound_by_name[i],
                in_bound_id,
            )

        # After creating all subscribers, use a poller to track them all
        for sub in self.p2p_subs.values():
            self.sub_poller.register(sub._zmq_socket, zmq.POLLIN)

        # Then start a thread to read the sub poller
        self.poll_inputs_thread = threading.Thread(target=self.poll_inputs)
        self.poll_inputs_thread.start()

    def poll_inputs(self):

        while self.node.running:

            # self.logger.debug(f"{self}: polling inputs")

            # Wait until we get data from any of the subscribers
            events = dict(self.sub_poller.poll(timeout=1000))

            # Empty if no events
            if len(events) == 0:
                continue

            # self.logger.debug(f"{self}: polling event processing {len(events)}")

            # Default value
            follow_event = False

            # Else, update values
            for s in events:  # socket

                # self.logger.debug(f"{self}: processing event {s}")

                # Update
                name, id = self.socket_to_sub_name_mapping[s]  # inbound
                serial_data_chunk = s.recv()
                self.in_bound_data[name] = DataChunk.from_bytes(serial_data_chunk)

                # Update flag if new values are coming from the node that is
                # being followed
                if self.follow == id:
                    follow_event = True

            # self.logger.debug(
            #     f"{self}: polling {self.in_bound_data}, follow = {follow_event}, event= {events}"
            # )

            # If update on the follow and all inputs available, then use the inputs
            if follow_event and all(
                [type(x) != type(None) for x in self.in_bound_data.values()]
            ):
                self.inputs_ready.set()
                # self.logger.debug(f"{self}: got inputs")
