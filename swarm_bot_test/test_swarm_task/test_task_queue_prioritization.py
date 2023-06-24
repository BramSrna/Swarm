import logging
import unittest
import time

from unittest.mock import MagicMock

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_task.task_executor_pool import TaskExecutorPool
from swarm.swarm_task.task_scheduling_algorithms import simple_task_sort


class SimpleTask(SwarmTask):
    def __init__(self, extra_params):
        super().__init__(extra_params)
        self.priority_score = extra_params[0]

        self.task_complete = False
        self.sleep_time = 3

    def is_complete(self):
        return self.task_complete

    def execute_task(self):
        time.sleep(self.sleep_time)
        self.task_complete = True

    def get_task_output(self):
        return self.sleep_time

    def get_priority_score(self):
        return self.priority_score


class TestTaskQueuePrioritization(NetworkNodeTestClass):
    def test_swarm_bot_will_prioritize_task_queue_starting_with_highest_priority_item(self):
        test_task_highest_priority = SimpleTask([1])
        test_task_med_priority = SimpleTask([0.5])
        test_task_low_priority = SimpleTask([0])

        def mock_read(arg):
            ret_map = {
                "TASK_QUEUE/" + str(test_task_highest_priority.get_id()): {"TASK": test_task_highest_priority},
                "TASK_QUEUE/" + str(test_task_med_priority.get_id()): {"TASK": test_task_med_priority},
                "TASK_QUEUE/" + str(test_task_low_priority.get_id()): {"TASK": test_task_low_priority}
            }
            return ret_map[arg]

        test_executor_interface = MagicMock()
        test_executor_interface.read_from_swarm_memory = MagicMock()
        test_executor_interface.read_from_swarm_memory.side_effect = mock_read
        test_task_executor_pool = TaskExecutorPool(test_executor_interface, 1, 1, simple_task_sort)

        test_task_executor_pool.set_task_executor_status(False)

        test_task_executor_pool.task_queue_monitor("TEST_PATH", {
                str(test_task_highest_priority.get_id()): test_task_highest_priority,
                str(test_task_med_priority.get_id()): test_task_med_priority,
                str(test_task_low_priority.get_id()): test_task_low_priority
        })

        first_task = test_task_executor_pool.get_info_of_next_task_to_execute()
        
        self.assertEqual({"TASK": test_task_highest_priority}, first_task)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
