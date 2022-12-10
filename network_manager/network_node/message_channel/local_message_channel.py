from network_manager.network_node.message_channel.message_channel import MessageChannel
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper
from network_manager.network_node.message_channel.message_channel_user import MessageChannelUser

"""
LocalMessageChannel

A message channel that can be used to send messages between nodes when
the nodes are in the same execution environment.

"""


class LocalMessageChannel(MessageChannel):
    def __init__(self, source_node: MessageChannelUser, target_node: MessageChannelUser):
        """
        __init__

        Creates a new LocalMessageChannel object

        @param source_node [MessageChannelUser] The MessageChannelUser sending the message
        @param target_node [MessageChannelUser] The MessageChannelUser receiving the message

        @return [LocalMessageChannel] The created LocalMessageChannel
        """

        if not isinstance(source_node, MessageChannelUser):
            raise Exception("ERROR: Source node must implement the MessageChannelUser class")

        if not isinstance(target_node, MessageChannelUser):
            raise Exception("ERROR: target node must implement the MessageChannelUser class")

        self.source_node = source_node
        self.target_node = target_node

    def send_message(self, message: MessageWrapper) -> None:
        """
        send_message

        Send the given message accross the LocalMessageChannel

        @param message [MessageWrapper] The message to send

        @return None
        """
        self.target_node.receive_message(message)
