from abc import ABC, abstractmethod
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper

"""
MessageChannel

The abstract class for defining message channel classes
"""


class MessageChannel(ABC):
    def __init__(self):
        """
        init

        Creates a new MessageChannel object

        @param None

        @return [MessageChannel] The new MessageChannel object
        """
        pass

    @abstractmethod
    def send_message(self, message: MessageWrapper) -> None:
        """
        send_message

        Send the given message accross the MessageChannel

        @param message_payload [MessageWrapper] The message to send

        @return None
        """
        pass
