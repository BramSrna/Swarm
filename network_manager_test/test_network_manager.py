import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from network_manager.network_node.network_node import NetworkNode
from network_manager.network_manager import NetworkManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel


class TestNetworkManager(NetworkNodeTestClass):
    def setUp(self):
        super().setUp()
        self.test_network_managers = []

    def tearDown(self):
        for network_manager in self.test_network_managers:
            network_manager.teardown()

    def create_network_manager(self, connectivity_type):
        new_manager = NetworkManager(connectivity_type)
        self.test_network_managers.append(new_manager)
        return new_manager

    def test_new_node_added_in_fully_connected_network_will_be_connected_to_all_other_nodes(self):
        test_network_manager = self.create_network_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)
        test_network_node_3 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()
        test_network_node_3.startup()

        test_network_manager.add_network_node(test_network_node_1)
        test_network_manager.add_network_node(test_network_node_2)
        test_network_manager.add_network_node(test_network_node_3)

        new_network_node = self.create_network_node(NetworkNode)

        new_network_node.startup()

        test_network_manager.add_network_node(new_network_node)

        self.assertTrue(new_network_node.is_connected_to(test_network_node_1.get_id()))
        self.assertTrue(new_network_node.is_connected_to(test_network_node_2.get_id()))
        self.assertTrue(new_network_node.is_connected_to(test_network_node_3.get_id()))

        self.assertTrue(test_network_node_1.is_connected_to(new_network_node.get_id()))
        self.assertTrue(test_network_node_2.is_connected_to(new_network_node.get_id()))
        self.assertTrue(test_network_node_3.is_connected_to(new_network_node.get_id()))

    def test_new_node_added_in_partially_connected_network_will_be_connected_to_random_node(self):
        test_network_manager = self.create_network_manager(NetworkConnectivityLevel.PARTIALLY_CONNECTED)

        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)
        test_network_node_3 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()
        test_network_node_3.startup()

        test_network_manager.add_network_node(test_network_node_1)
        test_network_manager.add_network_node(test_network_node_2)
        test_network_manager.add_network_node(test_network_node_3)

        new_network_node = self.create_network_node(NetworkNode)

        new_network_node.startup()

        test_network_manager.add_network_node(new_network_node)

        connections = new_network_node.get_connections()

        self.assertEqual(1, len(connections))
        self.assertIn(connections[0], [test_network_node_1.get_id(), test_network_node_2.get_id(), test_network_node_3.get_id()])

    def test_new_node_added_in_centralized_network_will_be_connected_to_central_network_node(self):
        test_network_manager = self.create_network_manager(NetworkConnectivityLevel.CENTRALIZED)

        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)
        test_network_node_3 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()
        test_network_node_3.startup()

        test_network_manager.add_network_node(test_network_node_1)
        test_network_manager.add_network_node(test_network_node_2)
        test_network_manager.add_network_node(test_network_node_3)

        new_network_node = self.create_network_node(NetworkNode)

        new_network_node.startup()

        test_network_manager.add_network_node(new_network_node)

        connections = new_network_node.get_connections()

        self.assertEqual(1, len(connections))
        self.assertEqual(test_network_manager.get_central_network_node().get_id(), connections[0])

        self.assertTrue(test_network_manager.get_central_network_node().is_connected_to(new_network_node.get_id()))

    def test_removing_node_from_network_will_disconnect_it_from_all_nodes(self):
        test_network_manager = self.create_network_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)
        test_network_node_3 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()
        test_network_node_3.startup()

        test_network_manager.add_network_node(test_network_node_1)
        test_network_manager.add_network_node(test_network_node_2)
        test_network_manager.add_network_node(test_network_node_3)

        new_network_node = self.create_network_node(NetworkNode)

        new_network_node.startup()

        test_network_manager.add_network_node(new_network_node)

        self.assertTrue(new_network_node.is_connected_to(test_network_node_1.get_id()))
        self.assertTrue(new_network_node.is_connected_to(test_network_node_2.get_id()))
        self.assertTrue(new_network_node.is_connected_to(test_network_node_3.get_id()))

        self.assertTrue(test_network_node_1.is_connected_to(new_network_node.get_id()))
        self.assertTrue(test_network_node_2.is_connected_to(new_network_node.get_id()))
        self.assertTrue(test_network_node_3.is_connected_to(new_network_node.get_id()))

        test_network_manager.remove_network_node(new_network_node.get_id())

        self.assertFalse(new_network_node.is_connected_to(test_network_node_1.get_id()))
        self.assertFalse(new_network_node.is_connected_to(test_network_node_2.get_id()))
        self.assertFalse(new_network_node.is_connected_to(test_network_node_3.get_id()))

        self.assertFalse(test_network_node_1.is_connected_to(new_network_node.get_id()))
        self.assertFalse(test_network_node_2.is_connected_to(new_network_node.get_id()))
        self.assertFalse(test_network_node_3.is_connected_to(new_network_node.get_id()))

    def test_network_manager_will_throw_an_error_if_removing_a_node_will_leave_orphan_nodes(self):
        test_network_manager = self.create_network_manager(NetworkConnectivityLevel.CENTRALIZED)

        test_network_node_1 = self.create_network_node(NetworkNode)
        test_network_node_2 = self.create_network_node(NetworkNode)
        test_network_node_3 = self.create_network_node(NetworkNode)

        test_network_node_1.startup()
        test_network_node_2.startup()
        test_network_node_3.startup()

        test_network_manager.add_network_node(test_network_node_1)
        test_network_manager.add_network_node(test_network_node_2)
        test_network_manager.add_network_node(test_network_node_3)

        new_network_node = self.create_network_node(NetworkNode)

        new_network_node.startup()

        test_network_manager.add_network_node(new_network_node)

        with self.assertRaises(Exception) as raised_error:
            test_network_manager.remove_network_node(test_network_manager.get_central_network_node().get_id())

        self.assertIn("Removing node from network would leave an orphaned node", str(raised_error.exception))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
