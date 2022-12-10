import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from network_manager.network_node.network_node import NetworkNode


class TestNetworkMessaging(NetworkNodeTestClass):
    def test_node_will_receive_sent_message(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_2.connect_to_network_node(test_network_node_1)

        msg_id = test_network_node_1.create_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_network_node_1.sent_msg_with_id(msg_id))
        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))

    def test_messages_can_be_sent_after_node_is_torn_down_and_started_again(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_2.connect_to_network_node(test_network_node_1)

        test_network_node_1.teardown()
        test_network_node_1.startup()

        msg_id = test_network_node_1.create_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_network_node_1.sent_msg_with_id(msg_id))
        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))

    def test_message_handler_associated_with_message_type_will_be_called_when_message_is_received(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_2.connect_to_network_node(test_network_node_1)

        test_send_message_type = "SEND"
        test_response_message_type = "RESPONSE"

        def TestHandler(network_node, message):
            network_node.create_directed_message(message.get_sender_id(), test_response_message_type, {})

        test_network_node_2.assign_msg_handler(test_send_message_type, TestHandler)

        test_network_node_1.create_directed_message(test_network_node_2.get_id(), test_send_message_type, {})

        self.wait_for_idle_network()

        received_messages = test_network_node_1.get_received_messages()
        self.assertTrue(test_response_message_type, received_messages[list(received_messages.keys())[0]][0])

    def test_cannot_send_a_message_to_a_node_after_disconnecting_from_it(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_2.connect_to_network_node(test_network_node_1)

        msg_id = test_network_node_1.create_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_network_node_1.sent_msg_with_id(msg_id))
        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))

        test_network_node_1.disconnect_from_network_node(test_network_node_2.get_id())
        test_network_node_2.disconnect_from_network_node(test_network_node_1.get_id())

        with self.assertRaises(Exception) as raised_error:
            msg_id = test_network_node_1.create_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.assertIn("Tried to create message for unknown node ID", str(raised_error.exception))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
