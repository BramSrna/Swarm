# https://github.com/BramSrna/NetworkManager/pull/23/files
import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.task_scheduling_algorithms import simple_task_sort
from swarm.local_swarm_memory import LocalSwarmMemory


class SwarmBot(NetworkNode):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        NetworkNode.__init__(self, additional_config_path=os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"), additional_config_dict=additional_config_dict)

        self.assigned_task = None

        self.run_task_executor = threading.Event()
        self.task_bundle_queue_has_values = threading.Event()
        self.task_execution_listeners = []
        self.task_execution_history = []

        self.local_swarm_memory = LocalSwarmMemory(self.get_id())

        self.execution_group = {}

        task_scheduling_algorithms = {
            "SimpleTaskSort": simple_task_sort
        }

        self.max_task_executions = self.config["max_task_executions"]
        self.task_scheduling_algorithm = task_scheduling_algorithms[self.config["task_scheduling_algorithm"]]

        self.msg_handler_dict = {
            str(MessageTypes.SWARM_MEMORY_OBJECT_LOCATION): self.handle_swarm_memory_object_location_message,
            str(MessageTypes.REQUEST_SWARM_MEMORY_READ): self.handle_request_swarm_memory_read_message,
            str(MessageTypes.TRANSFER_SWARM_MEMORY_VALUE): self.handle_transfer_swarm_memory_value_message,
            str(MessageTypes.DELETE_FROM_SWARM_MEMORY): self.handle_delete_from_swarm_memory_message
        }
        for msg_type, handler in self.msg_handler_dict.items():
            self.assign_msg_handler(msg_type, handler)

    def startup(self):
        NetworkNode.startup(self)
        self.start_task_executor()

    def teardown(self):
        NetworkNode.teardown(self)
        self.task_bundle_queue_has_values.set()

    def start_task_executor(self):
        thread = threading.Thread(target=self.task_executor_loop)
        thread.start()

    def add_task_execution_listener(self, new_listener):
        self.task_execution_listeners.append(new_listener)

    def get_assigned_task(self):
        return self.assigned_task

    def validate_task_bundle(self, task_bundle):
        if task_bundle.get_req_num_bots() - 1 > len(self.msg_channels):
            return False
        return True

    def receive_task_bundle(self, new_task_bundle):
        if not self.validate_task_bundle(new_task_bundle):
            return False

        self.write_to_swarm_memory(new_task_bundle.get_id(), new_task_bundle, new_task_bundle.__class__.__name__)
        return True

    def write_to_swarm_memory(self, key_to_write, value_to_write, data_type):
        self.local_swarm_memory.write(key_to_write, value_to_write, data_type)
        self.send_propagation_message(MessageTypes.SWARM_MEMORY_OBJECT_LOCATION, {"OBJECT_ID": key_to_write, "LOCATION_ID": self.get_id(), "DATA_TYPE": data_type})
        if data_type == "SwarmTaskBundle":
            self.task_bundle_queue_has_values.set()

    def read_from_swarm_memory(self, key_to_read):
        bot_with_obj = self.local_swarm_memory.get_data_holder_id(key_to_read)
        if bot_with_obj is None:
            return None
        elif bot_with_obj == self.get_id():
            return self.local_swarm_memory.read(key_to_read)
        else:
            # TODO: Add cache clearing mechanism and make cache more robust
            self.local_swarm_memory.prepare_cache_spot(key_to_read)

            self.send_propagation_message(MessageTypes.REQUEST_SWARM_MEMORY_READ, {"KEY_TO_READ": key_to_read})
            return self.local_swarm_memory.wait_for_cache_value(key_to_read)

    def delete_from_swarm_memory(self, key_to_delete):
        self.local_swarm_memory.delete(key_to_delete)
        self.send_propagation_message(MessageTypes.DELETE_FROM_SWARM_MEMORY, {"KEY_TO_DELETE": key_to_delete})

    def get_task_execution_history(self):
        return self.task_execution_history

    def set_task_executor_status(self, new_status):
        if new_status and self.run_task_executor.is_set():
            self.run_task_executor.clear()
            self.start_task_executor()
        elif (not new_status) and (not self.run_task_executor.is_set()):
            self.run_task_executor.set()
            self.task_bundle_queue_has_values.set()
        else:
            self.logger.debug("New task executor status ({}) matches current status. No changes being made.".format(new_status))

    def get_next_task_to_execute(self):
        next_task_bundle = None
        task_bundle_queue = self.local_swarm_memory.get_ids_of_contents_of_type("SwarmTaskBundle")
        if len(task_bundle_queue) > 0:
            task_bundle_queue.sort(key=self.task_scheduling_algorithm)
            next_task_bundle_id = task_bundle_queue.pop(0)
            next_task_bundle = self.read_from_swarm_memory(next_task_bundle_id)
        return next_task_bundle

    def get_task_bundle_queue(self):
        return self.local_swarm_memory.get_ids_of_contents_of_type("SwarmTaskBundle")

    def handle_swarm_memory_object_location_message(self, message):
        msg_payload = message.get_message_payload()
        object_id = msg_payload["OBJECT_ID"]
        location_id = msg_payload["LOCATION_ID"]
        data_type = msg_payload["DATA_TYPE"]
        self.local_swarm_memory.update_data_holder(object_id, location_id, data_type)
        if data_type == "SwarmTaskBundle":
            self.task_bundle_queue_has_values.set()

    def handle_request_swarm_memory_read_message(self, message):
        msg_payload = message.get_message_payload()
        object_key = msg_payload["KEY_TO_READ"]

        if self.local_swarm_memory.has_data_key(object_key):
            object_value = self.local_swarm_memory.read(object_key)

            self.send_propagation_message(MessageTypes.TRANSFER_SWARM_MEMORY_VALUE, {"OBJECT_KEY": object_key, "OBJECT_VALUE": object_value})

    def handle_transfer_swarm_memory_value_message(self, message):
        msg_payload = message.get_message_payload()
        object_key = msg_payload["OBJECT_KEY"]
        object_value = msg_payload["OBJECT_VALUE"]

        self.local_swarm_memory.prepare_cache_spot(object_key)
        self.local_swarm_memory.write_cache_value(object_key, object_value)

    def handle_delete_from_swarm_memory_message(self, message):
        msg_payload = message.get_message_payload()
        key_to_delete = msg_payload["KEY_TO_DELETE"]

        self.local_swarm_memory.delete(key_to_delete)

    def task_executor_loop(self):
        while (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
            self.task_bundle_queue_has_values.wait()

            if (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
                self._notify_process_state(True)

                next_task_bundle = self.get_next_task_to_execute()

                if next_task_bundle is not None:
                    self.delete_from_swarm_memory(next_task_bundle.get_id())
                    self.assigned_task = next_task_bundle.get_tasks()[0]
                    self.task_execution_history.append(self.assigned_task)
                    executor_interface = ExecutorInterface(self)
                    self.assigned_task.setup(executor_interface, self.execution_group)
                    curr_execution = 0
                    max_executions = self.max_task_executions
                    while (not self.assigned_task.is_complete()) and (curr_execution < max_executions):
                        self.assigned_task.execute_task()
                        curr_execution += 1

                    for task_execution_listener in self.task_execution_listeners:
                        task_execution_listener.notify_task_completion(self.assigned_task.get_id(), self.assigned_task.get_task_output())

                    self.assigned_task = None
                else:
                    self.task_bundle_queue_has_values.clear()

                self._notify_process_state(False)
