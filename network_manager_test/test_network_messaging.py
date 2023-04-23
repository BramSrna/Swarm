import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from network_manager.network_node.network_node import NetworkNode


class TestNetworkMessaging(NetworkNodeTestClass):
    def test_node_will_receive_sent_message(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.connect_to_network_node(test_network_node_2)

        test_network_node_1.send_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        msg_id = list(test_network_node_1.get_sent_messages().keys())[0]

        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))

    def test_messages_can_be_sent_after_node_is_torn_down_and_started_again(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.connect_to_network_node(test_network_node_2)

        test_network_node_1.teardown()
        test_network_node_1.start_network_node()

        test_network_node_1.send_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        msg_id = list(test_network_node_1.get_sent_messages().keys())[0]
        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))

    def test_cannot_send_a_message_to_a_node_after_disconnecting_from_it(self):
        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)

        test_network_node_1.connect_to_network_node(test_network_node_2)

        test_network_node_1.send_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        msg_id = list(test_network_node_1.get_sent_messages().keys())[0]
        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))

        test_network_node_1.disconnect_from_network_node(test_network_node_2.get_id())
        test_network_node_2.disconnect_from_network_node(test_network_node_1.get_id())

        with self.assertRaises(Exception) as raised_error:
            msg_id = test_network_node_1.send_directed_message(test_network_node_2.get_id(), "TEST", {})

        self.assertIn("Tried to send a message to an unknown nod", str(raised_error.exception))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
