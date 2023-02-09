import sys
import time

from network_manager.network_node.propagation_strategy.propagation_strategy import PropagationStrategy
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper

"""
SmartPropagation

Implementation of the smart propagation strategy.
With this strategy, any received messages are sent
to all connected nodes that have not already received the message.
Additionally, the message is sent according to how long it
takes for each node to receive the message for a second time.
For example, assume this node is connected to two other nodes, node X and Y.
Node X normally receives the message for a second time 1 ms after receiving
it for the first time, while Node Y receives it for a second time after 10ms.
In this case, message will be sent to Node Y first. The reasoning behind this
is that this will prioritize sending the message to nodes where this node is
on the shortest path.
"""


class SmartPropagation(PropagationStrategy):
    def __init__(self, owner_network_node):
        """
        init

        Creates a new SmartPropagation object

        @param owner_network_node [NetworkNode] The network node that owns this object

        @return [SmartPropagation] The crated SmartPropagation object
        """
        super().__init__(owner_network_node)

        self.message_tracker = {}
        self.avg_receive_time_tracker = {}

        for node_id in self.network_node.get_message_channels().keys():
            self.add_node_to_tracker_if_not_present(node_id)

    def add_node_to_tracker_if_not_present(self, node_id: int) -> None:
        """
        add_node_to_tracker_if_not_present

        Adds the given node to the average receive time tracker
        if it is not already present in the tracker. Additionally,
        it initializes the average receive time for the node. This tracker
        is used to track how long it takes for each node to receive a
        message for the second time.

        @param node_id [int] The node to add to the tracker

        @return [None]
        """
        if node_id not in self.avg_receive_time_tracker:
            self.avg_receive_time_tracker[node_id] = {
                "NUM_RCVD_MSGS": 0,
                "CURR_AVG": sys.maxsize
            }

    def determine_prop_targets(self, message: MessageWrapper) -> list:
        """
        determine_prop_targets

        Determines the connected nodes that should receive
        the given message. In this strategy, any messages
        are sent to all connected nodes that have not already
        received the message. The messages are sent according
        to how long it takes for each node to receive the message
        for the second time.

        @param message [MessageWrapper] The message to send

        @return [list] The list of nodes to send the message to
        """
        targets = []
        for node_id in self.network_node.get_message_channels().keys():
            if (message is None) or (node_id != message.get_sender_id()):
                targets.append(node_id)
            self.add_node_to_tracker_if_not_present(node_id)

        sorted_targets = sorted(targets, key=lambda node_id: self.avg_receive_time_tracker[node_id]["CURR_AVG"], reverse=True)
        return sorted_targets

    def track_message_propagation(self, message: MessageWrapper) -> None:
        """
        track_message_propagation

        Called when a previously propagated message is received again.
        Updates the average receive time tracker according to the time
        this method was called.

        @param message [MessageWrapper] The received message

        @return [None]
        """
        message_id = message.get_id()
        time_received = time.time()

        if message not in self.message_tracker:
            self.message_tracker[message_id] = time_received

        time_receive_diff = time_received - self.message_tracker[message_id]

        sender_id = message.get_sender_id()

        self.add_node_to_tracker_if_not_present(sender_id)

        curr_percent = self.avg_receive_time_tracker[sender_id]["NUM_RCVD_MSGS"] / 100
        new_avg = (self.avg_receive_time_tracker[sender_id]["CURR_AVG"] * curr_percent) + \
            (time_receive_diff * (1 - curr_percent))
        self.avg_receive_time_tracker[sender_id]["CURR_AVG"] = new_avg
        self.avg_receive_time_tracker[sender_id]["NUM_RCVD_MSGS"] += 1
