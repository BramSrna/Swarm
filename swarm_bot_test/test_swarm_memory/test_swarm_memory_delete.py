import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot


class TestSwarmMemoryDelete(NetworkNodeTestClass):
    def test_can_delete_swarm_memory_objects_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)

        self.assertEqual(original_val, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

        test_swarm_bot_1.delete_from_swarm_memory(test_mem_id)

        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_delete_swarm_memory_objects_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)

        self.wait_for_idle_network()
        self.assertEqual(original_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))

        test_swarm_bot_2.delete_from_swarm_memory(test_mem_id)

        self.wait_for_idle_network()
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_can_delete_swarm_memory_objects_when_info_stored_on_non_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)

        self.wait_for_idle_network()
        self.assertEqual(original_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

        test_swarm_bot_3.delete_from_swarm_memory(test_mem_id)

        self.wait_for_idle_network()
        self.assertEqual(None, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_deleting_parent_path_also_deletes_all_child_paths(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        parent_path = "PARENT_PATH"

        child_path = "CHILD_PATH"
        test_val = "CHILD_VAL"

        test_swarm_bot_1.write_to_swarm_memory(parent_path + "/" + child_path + "_1", test_val + "_1")
        test_swarm_bot_2.write_to_swarm_memory(parent_path + "/" + child_path + "_2", test_val + "_2")
        test_swarm_bot_3.write_to_swarm_memory(parent_path + "/" + child_path + "_3", test_val + "_3")

        time.sleep(1)
        test_swarm_bot_3.delete_from_swarm_memory(parent_path)

        self.wait_for_idle_network()
        self.assertEqual(None, test_swarm_bot_3.read_from_swarm_memory(parent_path + "/" + child_path + "_3"))
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(parent_path + "/" + child_path + "_2"))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(parent_path + "/" + child_path + "_1"))

        self.assertEqual(None, test_swarm_bot_3.read_from_swarm_memory(parent_path))
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(parent_path))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(parent_path))

    def test_deleting_child_path_will_not_delete_parent_path(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        parent_path = "PARENT_PATH"

        child_path = "CHILD_PATH"
        test_val = "CHILD_VAL"

        test_swarm_bot_1.write_to_swarm_memory(parent_path + "/" + child_path + "_1", test_val + "_1")
        test_swarm_bot_2.write_to_swarm_memory(parent_path + "/" + child_path + "_2", test_val + "_2")
        test_swarm_bot_3.write_to_swarm_memory(parent_path + "/" + child_path + "_3", test_val + "_3")

        time.sleep(1)

        test_swarm_bot_3.delete_from_swarm_memory(parent_path + "/" + child_path + "_1")

        self.wait_for_idle_network()
        self.assertEqual(test_val + "_3", test_swarm_bot_3.read_from_swarm_memory(parent_path + "/" + child_path + "_3"))
        self.assertEqual(test_val + "_2", test_swarm_bot_2.read_from_swarm_memory(parent_path + "/" + child_path + "_2"))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(parent_path + "/" + child_path + "_1"))

        expected_dict = {
            child_path + "_2": test_val + "_2",
            child_path + "_3": test_val + "_3"
        }

        self.assertEqual(expected_dict, test_swarm_bot_1.read_from_swarm_memory(parent_path))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
