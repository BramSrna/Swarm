from abc import ABC, abstractmethod
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper

"""
PropagationStrategy

Abstract class used to define new propagation strategies.
This class defines handlers and tracking methods that
the owner network node will use to propagate messages.
"""


class PropagationStrategy(ABC):
    def __init__(self, owner_network_node):
        """
        init

        Creates a new PropagationStrategy object

        @param owner_network_node [NetworkNode] The network node that owns this object

        @return [PropagationStrategy] The created PropagationStrategy object
        """
        self.network_node = owner_network_node

    @abstractmethod
    def determine_prop_targets(self, message: MessageWrapper) -> list:
        """
        determine_prop_targets

        Determines the connected nodes that should receive
        the given message.

        @param message [MessageWrapper] The message to send

        @return [list] The list of nodes to send the message to
        """
        pass

    @abstractmethod
    def track_message_propagation(self, message: MessageWrapper) -> None:
        """
        track_message_propagation

        Called when a previously propagated message is received again.

        @param message [MessageWrapper] The received message

        @return [None]
        """
        pass
