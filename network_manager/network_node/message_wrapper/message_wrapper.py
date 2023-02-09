from abc import ABC

"""
MessageWrapper

Abstract class for defining classes used to wrap messages
to send through MessageChannel objects
"""


class MessageWrapper(ABC):
    def __init__(
        self,
        msg_id: int,
        sender_id: int,
        target_node_id: int,
        message_type: str,
        message_payload: dict,
        propagation_flag: bool
    ):
        """
        init

        Creates a new MessageWrapper object

        @param msg_id [int] The ID of the message to send
        @param sender_id [int] The ID of the object sending the message
        @param target_node_id [int] The ID of the object to receive the message
        @param message_type [MessageTypes] The type of message to create
        @param message_payload [dict] The payload of the message to create
        @param propagation_flag [bool] True if the message should be propagated. False if the message should not be propageted.

        @return [MessageWrapper] The new MessageWrapper object
        """
        self.id = msg_id

        self.sender_id = sender_id
        self.target_node_id = target_node_id
        self.message_type = message_type
        self.message_payload = message_payload
        self.propagation_flag = propagation_flag

    def get_target_node_id(self) -> int:
        """
        get_target_node_id

        Returns the ID of the object to receive the message

        @param None

        @return [int] The ID of the object receiving the message
        """
        return self.target_node_id

    def get_message_type(self) -> str:
        """
        get_message_type

        Returns the message type

        @param None

        @return [str] The message type
        """
        return self.message_type

    def get_message_payload(self) -> dict:
        """
        get_message_payload

        Returns the message payload

        @param None

        @return [dict] The message payload
        """
        return self.message_payload

    def get_sender_id(self) -> int:
        """
        get_sender_id

        Returns the ID of the object that sent the message

        @param None

        @return [int] The ID of the sender
        """
        return self.sender_id

    def get_id(self) -> int:
        """
        get_id

        Returns the ID of the message

        @param None

        @return [int] The ID of the message
        """
        return self.id

    def get_propagation_flag(self) -> bool:
        """
        get_propagation_flag

        Returns the propagation flag value

        @param None

        @return [bool] The propagation value
        """
        return self.propagation_flag
