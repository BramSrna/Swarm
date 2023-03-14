import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot


class TestSwarmBotSwarmMemory(NetworkNodeTestClass):
    def test_can_access_swarm_memory_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val, "str")

        self.assertEqual(test_mem_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_access_swarm_memory_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val, "str")
        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))

    def test_can_access_swarm_memory_when_info_stored_on_non_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val, "str")
        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

    def test_can_update_swarm_memory_objects_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val, "str")

        self.assertEqual(original_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

        new_val = "TEST_VAL_2"

        test_swarm_bot_1.update_swarm_memory(test_mem_id, new_val)

        self.assertEqual(new_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_update_swarm_memory_objects_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val, "str")
        self.wait_for_idle_network()

        self.assertEqual(original_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))

        new_val = "TEST_VAL_2"

        test_swarm_bot_2.update_swarm_memory(test_mem_id, new_val)
        self.wait_for_idle_network()

        self.assertEqual(new_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))
        self.assertEqual(new_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_update_swarm_memory_objects_when_info_stored_on_non_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val, "str")
        self.wait_for_idle_network()

        self.assertEqual(original_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

        new_val = "TEST_VAL_2"

        test_swarm_bot_3.update_swarm_memory(test_mem_id, new_val)
        self.wait_for_idle_network()

        self.assertEqual(new_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))
        self.assertEqual(new_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))
        self.assertEqual(new_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))

    def test_can_delete_swarm_memory_objects_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val, "str")

        self.assertEqual(original_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

        self.assertEqual(original_val, test_swarm_bot_1.pop_from_swarm_memory(test_mem_id))

        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_delete_swarm_memory_objects_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val, "str")
        self.wait_for_idle_network()

        self.assertEqual(original_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))

        self.assertEqual(original_val, test_swarm_bot_2.pop_from_swarm_memory(test_mem_id))
        self.wait_for_idle_network()

        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_delete_swarm_memory_objects_when_info_stored_on_non_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val, "str")
        self.wait_for_idle_network()

        self.assertEqual(original_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

        self.assertEqual(original_val, test_swarm_bot_3.pop_from_swarm_memory(test_mem_id))
        self.wait_for_idle_network()

        self.assertEqual(None, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
