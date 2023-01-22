# https://github.com/BramSrna/NetworkManager/pull/23/files
import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.swarm_bot_message_handlers import handle_new_task_message, handle_request_task_transfer_message, handle_task_transfer_message


class SwarmBot(NetworkNode):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        NetworkNode.__init__(self, additional_config_path=os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"), additional_config_dict=additional_config_dict)

        self.assigned_task = None
        self.task_bundle_queue = []

        self.task_execution_history = []
        self.task_bundle_queue_has_values = threading.Event()
        self.task_execution_listeners = []

        self.execution_group = {}

        self.max_task_executions = self.config["max_task_executions"]

        self.msg_handler_dict = {
            str(MessageTypes.NEW_TASK): handle_new_task_message,
            str(MessageTypes.REQUEST_TASK_TRANSFER): handle_request_task_transfer_message,
            str(MessageTypes.TASK_TRANSFER): handle_task_transfer_message
        }
        for msg_type, handler in self.msg_handler_dict.items():
            self.assign_msg_handler(msg_type, handler)

    def startup(self):
        NetworkNode.startup(self)
        thread = threading.Thread(target=self.task_executor_loop)
        thread.start()

    def teardown(self):
        NetworkNode.teardown(self)
        self.task_bundle_queue_has_values.set()

    def add_task_execution_listener(self, new_listener):
        self.task_execution_listeners.append(new_listener)

    def get_assigned_task(self):
        return self.assigned_task

    def receive_task_bundle(self, new_task_bundle):
        if new_task_bundle.get_req_num_bots() > len(self.msg_channels):
            return False
        self.task_bundle_queue.append({"TASK": new_task_bundle})
        if len(self.task_bundle_queue) > 1:
            self.create_propagation_message(MessageTypes.NEW_TASK_BUNDLE, {"TASK_BUNDLE_ID": new_task_bundle.get_id(), "TASK_BUNDLE_HOLDER": self.get_id()})
        self.task_bundle_queue_has_values.set()
        return True

    def get_task_bundle_queue(self):
        return self.task_bundle_queue

    def get_task_execution_history(self):
        return self.task_execution_history

    def task_executor_loop(self):
        while (not self.run_node.is_set()):
            self.task_bundle_queue_has_values.wait()
            while (len(self.task_bundle_queue) > 0) and (not self.run_node.is_set()):
                self._notify_process_state(True)
                next_task_bundle = None

                while (next_task_bundle is None) and len(self.task_bundle_queue) > 0:
                    next_task_bundle_info = self.task_bundle_queue.pop(0)
                    if "TASK" in next_task_bundle_info:
                        next_task = next_task_bundle_info["TASK"]
                    else:
                        self.create_directed_message(next_task_bundle_info["HOLDER_ID"], MessageTypes.REQUEST_TASK_BUNDLE_TRANSFER, {"TASK_BUNDLE_ID": next_task_bundle_info["TASK_BUNDLE_ID"]})

                if next_task_bundle is not None:
                    self.assigned_task = next_task
                    self.task_execution_history.append(self.assigned_task)
                    executor_interface = ExecutorInterface(self)
                    self.assigned_task.setup(executor_interface, self.execution_group)
                    curr_execution = 0
                    max_executions = self.max_task_executions
                    while (not self.assigned_task.is_task_complete()) and (curr_execution < max_executions):
                        self.assigned_task.execute_task()
                        curr_execution += 1

                    for task_execution_listener in self.task_execution_listeners:
                        task_execution_listener.notify_task_completion(self.assigned_task.get_id(), self.assigned_task.get_task_output())

                    self.assigned_task = None

                self._notify_process_state(False)

            if len(self.task_bundle_queue) == 0:
                self.task_bundle_queue_has_values.clear()
