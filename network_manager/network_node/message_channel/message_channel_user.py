from abc import ABC, abstractmethod

"""
MessageChannelUser

The abstract class implemented by classes that want to use
MessageChannel objects

"""


class MessageChannelUser(ABC):
    def __init__(self):
        """
        init

        Creates a new MessageChannelUser

        @param None

        @return [MessageChannelUser] The created MessageChannelUser
        """
        pass

    @abstractmethod
    def receive_message(self, sender_id: int, message_type: str, message_payload: dict) -> None:
        """
        receive_message

        Wrapper method to handle receiving messages sent from another MessageChannelUser

        @param sender_id [int] The ID of the sender MessageChannelUser
        @param message_type [str] The type of message being sent
        @param message_payload [dict] The payload of the message to send

        @return None
        """
        pass
