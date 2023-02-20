import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.swarm_task.task_scheduling_algorithms import simple_task_sort
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_memory.swarm_memory_interface import SwarmMemoryInterface
from swarm.swarm_task.task_execution_controller import TaskExecutionController


class SwarmBot(NetworkNode):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        NetworkNode.__init__(
            self,
            additional_config_path=os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"),
            additional_config_dict=additional_config_dict
        )

        self.run_task_executor = threading.Event()
        self.task_queue_has_values = threading.Event()

        self.task_execution_history = []
        self.execution_group_ledger = {}

        self.swarm_memory_interface = SwarmMemoryInterface(ExecutorInterface(self))

        self.max_num_task_executors = 1
        self.task_executors = {}

        task_scheduling_algorithms = {
            "SimpleTaskSort": simple_task_sort
        }

        self.max_task_executions = self.config["max_task_executions"]
        self.task_scheduling_algorithm = task_scheduling_algorithms[self.config["task_scheduling_algorithm"]]

        self.assign_msg_handler(
            str(MessageTypes.SWARM_MEMORY_OBJECT_LOCATION),
            self.handle_swarm_memory_object_location_message
        )
        self.assign_msg_handler(str(MessageTypes.REQUEST_SWARM_MEMORY_READ), self.handle_request_swarm_memory_read_message)
        self.assign_msg_handler(str(MessageTypes.POP_FROM_SWARM_MEMORY), self.handle_pop_from_swarm_memory_message)
        self.assign_msg_handler(str(MessageTypes.EXECUTION_GROUP_CREATION), self.handle_execution_group_creation_message)
        self.assign_msg_handler(
            str(MessageTypes.REQUEST_JOIN_EXECUTION_GROUP),
            self.handle_request_join_execution_group_message
        )
        self.assign_msg_handler(str(MessageTypes.START_TASK_EXECUTION), self.handle_start_task_execution_message)
        self.assign_msg_handler(str(MessageTypes.EXECUTION_GROUP_TEARDOWN), self.handle_execution_group_teardown_message)
        self.assign_msg_handler(str(MessageTypes.DELETE_FROM_SWARM_MEMORY), self.handle_delete_from_swarm_memory_message)
        self.assign_msg_handler(str(MessageTypes.TASK_OUTPUT), self.handle_task_output_message)
        self.assign_msg_handler(str(MessageTypes.EXECUTION_GROUP_DELETION), self.handle_execution_group_deletion_message)

    def startup(self):
        NetworkNode.startup(self)
        self.start_task_executor()

    def teardown(self):
        NetworkNode.teardown(self)
        self.task_queue_has_values.set()

    def start_task_executor(self):
        thread = threading.Thread(target=self.task_executor_loop)
        thread.start()

    def receive_task_bundle(self, new_task_bundle, listener_bot_id=None):
        if new_task_bundle.get_req_num_bots() - 1 > len(self.msg_channels):
            return False

        tasks = new_task_bundle.get_tasks()
        for i in range(len(tasks)):
            curr_task = tasks[i]
            self.write_to_swarm_memory(
                curr_task.get_id(),
                {
                    "TASK": curr_task,
                    "PARENT_BUNDLE_ID": new_task_bundle.get_id(),
                    "REQ_NUM_BOTS": new_task_bundle.get_req_num_bots(),
                    "INDEX_IN_BUNDLE": i,
                    "LISTENER_ID": listener_bot_id
                },
                SwarmTask.__name__
            )
            self.task_queue_has_values.set()
        return True

    def read_from_swarm_memory(self, key_to_read):
        return self.swarm_memory_interface.read_from_swarm_memory(key_to_read)

    def write_to_swarm_memory(self, key_to_write, value_to_write, data_type):
        self.swarm_memory_interface.write_to_swarm_memory(key_to_write, value_to_write, data_type)

    def get_task_execution_history(self):
        return self.task_execution_history

    def set_task_executor_status(self, new_status):
        if new_status and self.run_task_executor.is_set():
            self.run_task_executor.clear()
            self.start_task_executor()
        elif (not new_status) and (not self.run_task_executor.is_set()):
            self.run_task_executor.set()
            self.task_queue_has_values.set()
        else:
            self.logger.debug("New task executor status ({}) matches current status. \
                No changes being made.".format(new_status))

    def get_task_bundle_queue(self):
        return self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)

    def get_task_queue(self):
        return self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)

    def get_next_task_to_execute(self):
        next_task_info = None
        task_queue = self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)
        while ((next_task_info is None) and (len(task_queue) > 0)):
            task_queue.sort(key=self.task_scheduling_algorithm)
            next_task_id = task_queue.pop(0)
            next_task_info = self.swarm_memory_interface.pop_from_swarm_memory(next_task_id)
            task_queue = self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)

        return next_task_info

    def get_execution_group_ledger(self):
        return self.execution_group_ledger

    def add_new_execution_group_leader(self, task_bundle_id, owner_id):
        self.execution_group_ledger[task_bundle_id] = owner_id

    def notify_task_completion(self, bundle_id):
        self.task_executors.pop(bundle_id)
        self.execution_group_ledger.pop(bundle_id)
        self.task_queue_has_values.set()
        self._notify_process_state(False)

    def handle_swarm_memory_object_location_message(self, message):
        msg_payload = message.get_message_payload()
        data_type = msg_payload["DATA_TYPE"]
        self.swarm_memory_interface.handle_swarm_memory_object_location_message(message)
        if data_type == SwarmTask.__name__:
            self.task_queue_has_values.set()

    def handle_request_swarm_memory_read_message(self, message):
        self.swarm_memory_interface.handle_request_swarm_memory_read_message(message)

    def handle_pop_from_swarm_memory_message(self, message):
        self.swarm_memory_interface.handle_pop_from_swarm_memory_message(message)

    def handle_delete_from_swarm_memory_message(self, message):
        self.swarm_memory_interface.handle_delete_from_swarm_memory_message(message)

    def handle_execution_group_creation_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        owner_id = msg_payload["OWNER_ID"]

        self.add_new_execution_group_leader(task_bundle_id, owner_id)
        if task_bundle_id in self.task_executors:
            self.task_executors[task_bundle_id].handle_execution_group_creation_message(message)

    def handle_request_join_execution_group_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id in self.task_executors:
            self.task_executors[task_bundle_id].handle_request_join_execution_group_message(message)

    def handle_start_task_execution_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id in self.task_executors:
            self.task_executors[task_bundle_id].handle_start_task_execution_message(message)

    def handle_execution_group_teardown_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id in self.task_executors:
            self.task_executors[task_bundle_id].handle_execution_group_teardown_message(message)

    def handle_task_output_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id in self.task_executors:
            self.task_executors[task_bundle_id].handle_task_output_message(message)

    def handle_execution_group_deletion_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id in self.execution_group_ledger:
            self.execution_group_ledger.pop(task_bundle_id)

    def task_executor_loop(self):
        while (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
            self.task_queue_has_values.wait()

            if (not self.run_node.is_set()) and (not self.run_task_executor.is_set()) and (len(self.task_executors.keys()) < self.max_num_task_executors):
                self._notify_process_state(True)

                next_task_info = self.get_next_task_to_execute()

                if next_task_info is None:
                    self.task_queue_has_values.clear()
                    self._notify_process_state(False)
                    continue

                next_task = next_task_info["TASK"]
                next_task_id = next_task.get_id()
                req_num_bots = next_task_info["REQ_NUM_BOTS"]
                bundle_id = next_task_info["PARENT_BUNDLE_ID"]
                index_in_bundle = next_task_info["INDEX_IN_BUNDLE"]
                listener_id = next_task_info["LISTENER_ID"]
                task_type = next_task.__class__.__name__

                self.task_executors[bundle_id] = TaskExecutionController(
                    bundle_id,
                    index_in_bundle,
                    task_type,
                    listener_id,
                    next_task,
                    next_task_id,
                    req_num_bots,
                    ExecutorInterface(self),
                    self.max_task_executions
                )
                self.task_executors[bundle_id].start_task_execution_process()

                self.task_execution_history.append(next_task)
