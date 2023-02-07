import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle
from swarm.message_types import MessageTypes


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

    def get_task_output(self):
        return self.sleep_time


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

    def test_swarm_can_execute_task_bundle_that_requires_multiple_bots(self):
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
        test_task_bundle.add_task(SimpleTask, 3)

        check_val = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(check_val)

        self.wait_for_idle_network()

        print(test_task_bundle.status_to_str())

        self.assertTrue(test_task_bundle.is_complete())

    def test_tasks_are_only_run_once(self):
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
        test_task_bundle.add_task(SimpleTask, 1)

        check_val = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(check_val)

        self.wait_for_idle_network()

        task = test_task_bundle.get_tasks()[0]

        executed_by_1 = (task in test_swarm_bot_1.get_task_execution_history()) and (task not in test_swarm_bot_2.get_task_execution_history()) and (task not in test_swarm_bot_3.get_task_execution_history())
        executed_by_2 = (task not in test_swarm_bot_1.get_task_execution_history()) and (task in test_swarm_bot_2.get_task_execution_history()) and (task not in test_swarm_bot_3.get_task_execution_history())
        executed_by_3 = (task not in test_swarm_bot_1.get_task_execution_history()) and (task not in test_swarm_bot_2.get_task_execution_history()) and (task in test_swarm_bot_3.get_task_execution_history())

        self.assertTrue(executed_by_1 or executed_by_2 or executed_by_3)

    def test_can_get_output_of_task_from_bot(self):
        listener_bot = self.create_network_node(SwarmBot)
        listener_bot.startup()

        listener_bot.set_task_executor_status(False)

        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_1.startup()

        test_swarm_bot_1.connect_to_network_node(listener_bot)
        listener_bot.connect_to_network_node(test_swarm_bot_1)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        can_execute = test_swarm_bot_1.receive_task_bundle(test_task_bundle, listener_bot_id=listener_bot.get_id())
        self.assertTrue(can_execute)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())

        rcvd_msgs = listener_bot.get_received_messages().values()
        task_outputs = []
        for msg in rcvd_msgs:
            if msg[0] == MessageTypes.TASK_OUTPUT:
                self.assertEqual(1, msg[1])
                self.assertEqual(3, msg[2].get_message_payload()["TASK_OUTPUT"])
                task_outputs.append(msg)
        self.assertGreater(len(task_outputs), 0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
