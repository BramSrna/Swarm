from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper

"""
LocalMessageWrapper

Class for defining message payloads to send using a LocalMessageChannel.
These messages will be sent between objects defined in the same execution environment.
"""


class LocalMessageWrapper(MessageWrapper):
    """
    init

    Creates a new LocalMessageWrapper object

    @param msg_id [int] The ID of the message being sent
    @param sender_id [int] The ID of the object sending the message
    @param target_node_id [int] The ID of the object to receive the message
    @param message_type [str] The type of message being sent
    @param message_payload [dict] The payload of the message to send
    @param propagation_flag [bool] True if the message should be propagated. False if the message should not be propageted.

    @return None
    """
    def __init__(
        self,
        msg_id: int,
        sender_id: int,
        target_node_id: int,
        message_type: str,
        message_payload: dict,
        propagation_flag: bool
    ) -> None:
        super().__init__(msg_id, sender_id, target_node_id, message_type, message_payload, propagation_flag)
