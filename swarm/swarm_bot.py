# https://github.com/BramSrna/NetworkManager/pull/23/files
import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.swarm_bot_message_handlers import handle_new_task_message, handle_request_task_transfer_message, handle_task_transfer_message

class SwarmBot(NetworkNode):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        NetworkNode.__init__(self, additional_config_path = os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"), additional_config_dict = additional_config_dict)

        self.assigned_task = None
        self.task_queue = []

        self.task_execution_history = []
        self.task_queue_has_values = threading.Event()

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
        self.task_queue_has_values.set()

    def get_assigned_task(self):
        return self.assigned_task

    def receive_task(self, new_task):
        self.task_queue.append({"TASK": new_task})
        if len(self.task_queue) > 1:
            self.create_propagation_message(MessageTypes.NEW_TASK, {"TASK_ID": new_task.get_id(), "TASK_HOLDER": self.get_id()})
        self.task_queue_has_values.set()

    def get_task_queue(self):
        return self.task_queue

    def get_task_execution_history(self):
        return self.task_execution_history

    def task_executor_loop(self):
        while (not self.run_node.is_set()):
            self.task_queue_has_values.wait()
            while (len(self.task_queue) > 0) and (not self.run_node.is_set()):
                self._notify_process_state(True)
                next_task = None
                while (next_task is None) and len(self.task_queue) > 0:
                    next_task_info = self.task_queue.pop(0)
                    if "TASK" in next_task_info:
                        next_task = next_task_info["TASK"]
                    else:
                        self.create_directed_message(next_task_info["HOLDER_ID"], MessageTypes.REQUEST_TASK_TRANSFER, {"TASK_ID": next_task_info["TASK_ID"]})
                if next_task is not None:
                    self.assigned_task = next_task

                    self.task_execution_history.append(self.assigned_task)
                    executor_interface = ExecutorInterface(self)
                    self.assigned_task.set_executor_interface(executor_interface)
                    curr_execution = 0
                    max_executions = self.max_task_executions
                    while (not self.assigned_task.is_task_complete()) and (curr_execution < max_executions):
                        self.assigned_task.execute_task()
                        curr_execution += 1
                    self.assigned_task = None
                self._notify_process_state(False)

            if len(self.task_queue) == 0:
                self.task_queue_has_values.clear()