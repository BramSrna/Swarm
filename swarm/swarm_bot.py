# https://github.com/BramSrna/NetworkManager/pull/23/files
import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.swarm_bot_message_handlers import handle_new_task_message, \
    handle_request_task_bundle_transfer_message, \
    handle_task_bundle_transfer_message, \
    handle_swarm_memory_object_location_message, \
    handle_request_swarm_memory_read_message, \
    handle_transfer_swarm_memory_value_message, \
    handle_delete_from_swarm_memory_message, \
    handle_notify_task_bundle_execution_start_message


class SwarmBot(NetworkNode):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        NetworkNode.__init__(self, additional_config_path=os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"), additional_config_dict=additional_config_dict)

        self.assigned_task = None
        self.task_bundle_queue = []

        self.task_execution_history = []
        self.run_task_executor = threading.Event()
        self.task_bundle_queue_has_values = threading.Event()
        self.task_execution_listeners = []

        self.local_swarm_memory_contents = {}
        self.swarm_mem_loc_hash = {}
        self.swarm_memory_cache = {}

        self.execution_group = {}

        self.max_task_executions = self.config["max_task_executions"]

        self.msg_handler_dict = {
            str(MessageTypes.NEW_TASK_BUNDLE): handle_new_task_message,
            str(MessageTypes.REQUEST_TASK_BUNDLE_TRANSFER): handle_request_task_bundle_transfer_message,
            str(MessageTypes.TASK_BUNDLE_TRANSFER): handle_task_bundle_transfer_message,
            str(MessageTypes.SWARM_MEMORY_OBJECT_LOCATION): handle_swarm_memory_object_location_message,
            str(MessageTypes.REQUEST_SWARM_MEMORY_READ): handle_request_swarm_memory_read_message,
            str(MessageTypes.TRANSFER_SWARM_MEMORY_VALUE): handle_transfer_swarm_memory_value_message,
            str(MessageTypes.DELETE_FROM_SWARM_MEMORY): handle_delete_from_swarm_memory_message,
            str(MessageTypes.NOTIFY_TASK_BUNDLE_EXECUTION_START): handle_notify_task_bundle_execution_start_message
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

        self.task_bundle_queue.append({"TASK": new_task_bundle})
        self.write_to_swarm_memory(new_task_bundle.get_id(), new_task_bundle)
        self.send_propagation_message(MessageTypes.NEW_TASK_BUNDLE, {"TASK_BUNDLE_ID": new_task_bundle.get_id(), "TASK_BUNDLE_HOLDER": self.get_id()})
        self.task_bundle_queue_has_values.set()
        return True

    def write_to_swarm_memory(self, key_to_write, value_to_write):
        self.local_swarm_memory_contents[key_to_write] = value_to_write
        self.swarm_mem_loc_hash[key_to_write] = self.get_id()
        self.send_propagation_message(MessageTypes.SWARM_MEMORY_OBJECT_LOCATION, {"OBJECT_ID": key_to_write, "LOCATION_ID": self.get_id()})

    def read_from_swarm_memory(self, key_to_read):
        if key_to_read not in self.swarm_mem_loc_hash:
            return None
        bot_with_obj = self.swarm_mem_loc_hash[key_to_read]
        if bot_with_obj == self.get_id():
            return self.local_swarm_memory_contents[key_to_read]
        else:
            # TODO: Add cache clearing mechanism and make cache more robust
            self.swarm_memory_cache[key_to_read] = {
                "LOCK": threading.Condition(),
                "VALUE": None
            }

            self.send_propagation_message(MessageTypes.REQUEST_SWARM_MEMORY_READ, {"KEY_TO_READ": key_to_read})
            if self.swarm_memory_cache[key_to_read]["VALUE"] is None:
                with self.swarm_memory_cache[key_to_read]["LOCK"]:
                    check = self.swarm_memory_cache[key_to_read]["LOCK"].wait(10)
                    if not check:
                        raise Exception("Memory value was not read in time. Key to read: {}".format(key_to_read))
                    else:
                        return self.swarm_memory_cache[key_to_read]["VALUE"]

    def delete_from_swarm_memory(self, key_to_delete):
        if key_to_delete in self.local_swarm_memory_contents:
            self.local_swarm_memory_contents.pop(key_to_delete)
        if key_to_delete in self.swarm_mem_loc_hash:
            self.swarm_mem_loc_hash.pop(key_to_delete)
        self.send_propagation_message(MessageTypes.DELETE_FROM_SWARM_MEMORY, {"KEY_TO_DELETE": key_to_delete})

    def get_task_bundle_queue(self):
        return self.task_bundle_queue

    def get_task_execution_history(self):
        return self.task_execution_history

    def set_task_executor_status(self, new_status):
        if new_status and self.run_task_executor.is_set():
            self.run_task_executor.clear()
            self.start_task_executor()
        elif (not new_status) and (not self.run_task_executor.is_set()):
            self.run_task_executor.set()
        else:
            self.logger.debug("New task executor status ({}) matches current status. No changes being made.".format(new_status))

    def task_executor_loop(self):
        while (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
            self.task_bundle_queue_has_values.wait()
            while (len(self.task_bundle_queue) > 0) and (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
                self._notify_process_state(True)
                next_task_bundle = None

                while (next_task_bundle is None) and len(self.task_bundle_queue) > 0:
                    next_task_bundle_info = self.task_bundle_queue.pop(0)
                    if "TASK" in next_task_bundle_info:
                        next_task_bundle = next_task_bundle_info["TASK"]
                        if not self.validate_task_bundle(next_task_bundle):
                            next_task_bundle = None
                    else:
                        self.send_directed_message(next_task_bundle_info["HOLDER_ID"], MessageTypes.REQUEST_TASK_BUNDLE_TRANSFER, {"TASK_BUNDLE_ID": next_task_bundle_info["TASK_BUNDLE_ID"]})

                if next_task_bundle is not None:
                    self.delete_from_swarm_memory(next_task_bundle.get_id())
                    self.send_propagation_message(MessageTypes.NOTIFY_TASK_BUNDLE_EXECUTION_START, {"TASK_BUNDLE_ID": next_task_bundle.get_id()})
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

                self._notify_process_state(False)

            if len(self.task_bundle_queue) == 0:
                self.task_bundle_queue_has_values.clear()
