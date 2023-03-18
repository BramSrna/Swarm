import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot
from swarm.swarm_memory.local_swarm_memory import LocalSwarmMemory


class TestSwarmBotSwarmMemory(NetworkNodeTestClass):
    def test_local_swarm_memory_write_and_read(self):
        test_instance = LocalSwarmMemory(0)

        test_instance.write("test", "val_1")
        self.assertEqual(test_instance.read("test"), "val_1")

        test_instance.write("test", "val_2")
        self.assertEqual(test_instance.read("test"), "val_2")

        test_instance.write("test/test2", "val_3")
        self.assertEqual(test_instance.read("test"), {"test2": "val_3"})

        test_instance.write("test/test2/test3", "val_3")
        self.assertEqual(test_instance.read("test"), {"test2": {"test3": "val_3"}})
        self.assertEqual(test_instance.read("test/test2"), {"test3": "val_3"})
        self.assertEqual(test_instance.read("test/test2/test3"), "val_3")

        test_instance.write("test", "val_1")
        self.assertEqual(test_instance.read("test"), "val_1")

    def test_local_swarm_memory_delete_and_read(self):
        test_instance = LocalSwarmMemory(0)

        test_instance.write("test", "val_1")
        self.assertTrue(test_instance.has_path("test"))
        test_instance.delete("test")
        self.assertFalse(test_instance.has_path("test"))
        self.assertEqual(test_instance.read("test"), None)

        test_instance.write("test/test2", "val_3")
        self.assertTrue(test_instance.has_path("test/test2"))
        test_instance.delete("test/test2")
        self.assertFalse(test_instance.has_path("test/test2"))
        self.assertEqual(test_instance.read("test/test2"), None)
        self.assertEqual(test_instance.read("test"), {})

        test_instance.write("test/test2/test3", "val_3")
        self.assertTrue(test_instance.has_path("test/test2/test3"))
        test_instance.write("test/test2/test4", "val_4")
        self.assertTrue(test_instance.has_path("test/test2/test4"))
        self.assertEqual(test_instance.read("test"), {"test2": {"test3": "val_3", "test4": "val_4"}})
        test_instance.delete("test/test2/test3")
        self.assertFalse(test_instance.has_path("test/test2/test3"))
        self.assertTrue(test_instance.has_path("test/test2/test4"))
        self.assertEqual(test_instance.read("test"), {"test2": {"test4": "val_4"}})

    def test_local_swarm_memory_get_key_holder_ids(self):
        test_instance = LocalSwarmMemory(0)

        test_instance.update_data_holder("test", 0)
        self.assertEqual(test_instance.get_key_holder_ids("test"), [0])

        test_instance.update_data_holder("test/test2", 0)
        test_instance.update_data_holder("test/test3", 1)
        self.assertEqual(test_instance.get_key_holder_ids("test"), [0, 1])

        test_instance.update_data_holder("test/test2/test3", 0)
        test_instance.update_data_holder("test/test2/test4", 1)
        test_instance.update_data_holder("test/test5/test6", 2)
        test_instance.update_data_holder("test/test5/test7", 3)
        self.assertEqual(test_instance.get_key_holder_ids("test"), [0, 1, 2, 3])

        test_instance.update_data_holder("test/test2/test3", 0)
        test_instance.update_data_holder("test/test2/test4", 1)
        test_instance.update_data_holder("test/test5/test6", 1)
        test_instance.update_data_holder("test/test5/test7", 2)
        self.assertEqual(test_instance.get_key_holder_ids("test"), [0, 1, 2])

    def test_can_access_swarm_memory_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_path = "str"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_path, test_mem_val)

        self.assertEqual(test_mem_val, test_swarm_bot_1.read_from_swarm_memory(test_path))

    def test_can_access_swarm_memory_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val)
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

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val)
        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

    def test_can_update_swarm_memory_objects_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_mem_id = "TEST_ID"
        original_val = "TEST_VAL_1"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)

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

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)
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

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)
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

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)

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

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)
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

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, original_val)
        self.wait_for_idle_network()

        self.assertEqual(original_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

        self.assertEqual(original_val, test_swarm_bot_3.pop_from_swarm_memory(test_mem_id))
        self.wait_for_idle_network()

        self.assertEqual(None, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id))

    def test_contents_written_to_one_table_are_unique_to_that_table(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        table_id_1 = "TABLE_ID_1"
        table_id_2 = "TABLE_ID_2"

        table_key_1 = "TABLE_KEY_1"

        table_val_1 = "TABLE_VAL_1"
        table_val_2 = "TABLE_VAL_2"

        test_swarm_bot_1.write_to_swarm_memory(table_id_1 + "/" + table_key_1, table_val_1)
        test_swarm_bot_1.write_to_swarm_memory(table_id_2 + "/" + table_key_1, table_val_2)

        self.assertEqual({table_key_1: table_val_1}, test_swarm_bot_1.read_from_swarm_memory(table_id_1))
        self.assertEqual({table_key_1: table_val_2}, test_swarm_bot_1.read_from_swarm_memory(table_id_2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
