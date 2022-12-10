from network_manager.network_node.propagation_strategy.propagation_strategy import PropagationStrategy
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper

"""
NaivePropagation

Implementation of the naive propagation strategy.
With this strategy, any received messages are sent
to all connected nodes to be propagated.
"""


class NaivePropagation(PropagationStrategy):
    def __init__(self, owner_network_node):
        """
        init

        Creates a new NaivePropagation object

        @param owner_network_node [NetworkNode] The network node that owns this object

        @return [NaivePropagation] The crated NaivePropagation object
        """
        super().__init__(owner_network_node)

    def determine_prop_targets(self, message: MessageWrapper) -> list:
        """
        determine_prop_targets

        Determines the connected nodes that should receive
        the given message. In this strategy, any messages
        are sent to all connected nodes.

        @param message [MessageWrapper] The message to send

        @return [list] The list of nodes to send the message to
        """
        return self.network_node.get_message_channels().keys()

    def track_message_propagation(self, message: MessageWrapper) -> None:
        """
        track_message_propagation

        Called when a previously propagated message is received again.
        Currently, this is not needed in the NaivePropagation strategy.

        @param message [MessageWrapper] The received message

        @return [None]
        """
        pass
