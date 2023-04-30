import logging
import unittest

from unittest.mock import MagicMock

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_memory.swarm_memory_interface import SwarmMemoryInterface


class TestSwarmMemoryInterface(NetworkNodeTestClass):
    def test_write_and_read(self):
        executor_interface = MagicMock()

        test_instance = SwarmMemoryInterface(executor_interface, 30, 100)

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

    def test_delete_and_read(self):
        executor_interface = MagicMock()

        test_instance = SwarmMemoryInterface(executor_interface, 30, 100)

        test_instance.write("test", "val_1")
        self.assertTrue(test_instance.path_exists_in_memory("test"))
        test_instance.delete("test")
        self.assertFalse(test_instance.path_exists_in_memory("test"))
        self.assertEqual(test_instance.read("test"), None)

        test_instance.write("test/test2", "val_3")
        self.assertTrue(test_instance.path_exists_in_memory("test/test2"))
        test_instance.delete("test/test2")
        self.assertFalse(test_instance.path_exists_in_memory("test/test2"))
        self.assertEqual(test_instance.read("test/test2"), None)
        self.assertEqual(test_instance.read("test"), {})

        test_instance.write("test/test2/test3", "val_3")
        self.assertTrue(test_instance.path_exists_in_memory("test/test2/test3"))
        test_instance.write("test/test2/test4", "val_4")
        self.assertTrue(test_instance.path_exists_in_memory("test/test2/test4"))
        self.assertEqual(test_instance.read("test"), {"test2": {"test3": "val_3", "test4": "val_4"}})
        test_instance.delete("test/test2/test3")
        self.assertFalse(test_instance.path_exists_in_memory("test/test2/test3"))
        self.assertTrue(test_instance.path_exists_in_memory("test/test2/test4"))
        self.assertEqual(test_instance.read("test"), {"test2": {"test4": "val_4"}})

    def test_can_write_contents_of_local_memory(self):
        executor_interface = MagicMock()

        test_instance = SwarmMemoryInterface(executor_interface, 30, 100)

        test_instance.write("test", "start")
        self.assertEqual(test_instance.read("test"), "start")
        test_instance.write("test", "middle")
        self.assertEqual(test_instance.read("test"), "middle")

        test_instance.write("test/inner", "start")
        self.assertEqual(test_instance.read("test/inner"), "start")
        test_instance.write("test/inner", "middle")
        self.assertEqual(test_instance.read("test/inner"), "middle")
        self.assertEqual(test_instance.read("test"), {"inner": "middle"})
        test_instance.write("test", "end")
        self.assertEqual(test_instance.read("test"), "end")
        self.assertEqual(test_instance.read("test/inner"), None)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
