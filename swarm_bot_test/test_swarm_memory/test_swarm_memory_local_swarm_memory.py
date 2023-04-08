import logging
import unittest

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
