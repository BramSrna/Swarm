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


class TestSwarmBot(NetworkNodeTestClass):
    def test_swarm_bot_will_execute_task_when_not_already_executing_a_task(self):
        test_swarm_bot = self.create_network_node(SwarmBot)

        test_swarm_bot.startup()

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        self.assertFalse(test_task_bundle.is_complete())

        test_swarm_bot.receive_task_bundle(test_task_bundle)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())

    def test_swarm_bot_will_pick_up_next_task_in_queue_when_done_executing_current_task(self):
        test_swarm_bot = self.create_network_node(SwarmBot)

        test_swarm_bot.startup()

        test_task_bundle_1 = SwarmTaskBundle()
        test_task_bundle_1.add_task(SimpleTask, 1)
        test_task_bundle_2 = SwarmTaskBundle()
        test_task_bundle_2.add_task(SimpleTask, 1)

        self.assertFalse(test_task_bundle_1.is_complete())
        self.assertFalse(test_task_bundle_2.is_complete())

        test_swarm_bot.receive_task_bundle(test_task_bundle_1)
        test_swarm_bot.receive_task_bundle(test_task_bundle_2)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle_1.is_complete())
        self.assertTrue(test_task_bundle_2.is_complete())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
