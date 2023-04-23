import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_memory.local_swarm_memory import LocalSwarmMemory


class TestSwarmMemoryLocalSwarmMemory(NetworkNodeTestClass):
    def test_local_swarm_memory_create_and_read(self):
        test_instance = LocalSwarmMemory(100)

        test_instance.create("test", "val_1")
        self.assertEqual(test_instance.read("test"), "val_1")

        test_instance.create("test", "val_2")
        self.assertEqual(test_instance.read("test"), "val_2")

        test_instance.create("test/test2", "val_3")
        self.assertEqual(test_instance.read("test"), {"test2": "val_3"})

        test_instance.create("test/test2/test3", "val_3")
        self.assertEqual(test_instance.read("test"), {"test2": {"test3": "val_3"}})
        self.assertEqual(test_instance.read("test/test2"), {"test3": "val_3"})
        self.assertEqual(test_instance.read("test/test2/test3"), "val_3")

        test_instance.create("test", "val_1")
        self.assertEqual(test_instance.read("test"), "val_1")

    def test_local_swarm_memory_delete_and_read(self):
        test_instance = LocalSwarmMemory(100)

        test_instance.create("test", "val_1")
        self.assertTrue(test_instance.has_path("test"))
        test_instance.delete("test")
        self.assertFalse(test_instance.has_path("test"))
        self.assertEqual(test_instance.read("test"), None)

        test_instance.create("test/test2", "val_3")
        self.assertTrue(test_instance.has_path("test/test2"))
        test_instance.delete("test/test2")
        self.assertFalse(test_instance.has_path("test/test2"))
        self.assertEqual(test_instance.read("test/test2"), None)
        self.assertEqual(test_instance.read("test"), {})

        test_instance.create("test/test2/test3", "val_3")
        self.assertTrue(test_instance.has_path("test/test2/test3"))
        test_instance.create("test/test2/test4", "val_4")
        self.assertTrue(test_instance.has_path("test/test2/test4"))
        self.assertEqual(test_instance.read("test"), {"test2": {"test3": "val_3", "test4": "val_4"}})
        test_instance.delete("test/test2/test3")
        self.assertFalse(test_instance.has_path("test/test2/test3"))
        self.assertTrue(test_instance.has_path("test/test2/test4"))
        self.assertEqual(test_instance.read("test"), {"test2": {"test4": "val_4"}})

    def test_can_update_contents_of_local_memory(self):
        test_instance = LocalSwarmMemory(100)

        test_instance.create("test", "start")
        self.assertEqual(test_instance.read("test"), "start")
        test_instance.update("test", "middle")
        self.assertEqual(test_instance.read("test"), "middle")

        test_instance.create("test/inner", "start")
        self.assertEqual(test_instance.read("test/inner"), "start")
        test_instance.update("test/inner", "middle")
        self.assertEqual(test_instance.read("test/inner"), "middle")
        self.assertEqual(test_instance.read("test"), {"inner": "middle"})
        test_instance.update("test", "end")
        self.assertEqual(test_instance.read("test"), "end")
        self.assertEqual(test_instance.read("test/inner"), None)

    def test_blocks_are_sorted_by_time_issued_when_multiple_blocks_are_received_for_the_same_state(self):
        test_instance = LocalSwarmMemory(100)
        
        test_instance.create("test", "start")
        test_instance.create("test", "start")
        test_instance.create("test", "start")

        test_instance.add_change_block(CreateBlock(0, 0, "test", "start"))
        test_instance.add_change_block(CreateBlock(0, 2, "test", "end"))
        test_instance.add_change_block(CreateBlock(0, 1, "test", "middle"))

        self.assertEqual(test_instance.read("test"), "end")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
