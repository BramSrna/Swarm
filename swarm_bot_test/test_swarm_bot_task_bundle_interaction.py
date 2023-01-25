import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_task_bundle import SwarmTaskBundle


class SimpleTask(SwarmTask):
    def __init__(self):
        super().__init__()
        self.task_complete = False
        self.sleep_time = 3

    def is_complete(self):
        return self.task_complete

    def execute_task(self):
        time.sleep(self.sleep_time)
        self.task_complete = True


class TestSwarmBotTaskBundleInteraction(NetworkNodeTestClass):
    def test_swarm_bot_will_reject_task_bundles_that_require_more_bots_than_the_swarm_contains(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)

        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_1)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_1)
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_2)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 5)

        self.assertFalse(test_swarm_bot_1.receive_task_bundle(test_task_bundle))

    def test_can_disable_task_execution(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_1.startup()

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        test_swarm_bot_1.set_task_executor_status(False)

        can_execute = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(can_execute)

        self.wait_for_idle_network()

        self.assertFalse(test_task_bundle.is_complete())

    def test_can_disable_and_reenable_task_execution(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_1.startup()

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        test_swarm_bot_1.set_task_executor_status(False)

        can_execute = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(can_execute)

        self.wait_for_idle_network()

        self.assertFalse(test_task_bundle.is_complete())

        test_swarm_bot_1.set_task_executor_status(True)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())

    def test_when_swarm_bot_receives_task_it_will_enter_each_tasks_queue(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_1)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_1)
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_2)

        test_swarm_bot_1.set_task_executor_status(False)
        test_swarm_bot_2.set_task_executor_status(False)
        test_swarm_bot_3.set_task_executor_status(False)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        check_val = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(check_val)

        self.wait_for_idle_network()

        self.assertGreater(len(test_swarm_bot_1.get_task_bundle_queue()), 0)
        self.assertGreater(len(test_swarm_bot_2.get_task_bundle_queue()), 0)
        self.assertGreater(len(test_swarm_bot_3.get_task_bundle_queue()), 0)

    def test_when_swarm_bot_executes_task_it_will_be_removed_from_all_bots_queues(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_1)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_1)
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_2)

        test_swarm_bot_1.set_task_executor_status(False)
        test_swarm_bot_2.set_task_executor_status(False)
        test_swarm_bot_3.set_task_executor_status(False)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        check_val = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(check_val)

        self.wait_for_idle_network()

        self.assertGreater(len(test_swarm_bot_1.get_task_bundle_queue()), 0)
        self.assertGreater(len(test_swarm_bot_2.get_task_bundle_queue()), 0)
        self.assertGreater(len(test_swarm_bot_3.get_task_bundle_queue()), 0)

        test_swarm_bot_1.set_task_executor_status(True)

        self.wait_for_idle_network()

        self.assertEqual(0, len(test_swarm_bot_1.get_task_bundle_queue()))
        self.assertEqual(0, len(test_swarm_bot_2.get_task_bundle_queue()))
        self.assertEqual(0, len(test_swarm_bot_3.get_task_bundle_queue()))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
