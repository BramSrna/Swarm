# https://github.com/BramSrna/NetworkManager/pull/23/files
import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.swarm_task.task_scheduling_algorithms import simple_task_sort
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_memory.swarm_memory_interface import SwarmMemoryInterface


class SwarmBot(NetworkNode):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        NetworkNode.__init__(
            self,
            additional_config_path=os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"),
            additional_config_dict=additional_config_dict
        )

        self.assigned_task = None
        self.executor_interface = ExecutorInterface(self)

        self.run_task_executor = threading.Event()
        self.task_queue_has_values = threading.Event()
        self.task_execution_listeners = []
        self.task_execution_history = []
        self.execution_group_ledger = {}
        self.execution_group_lock = threading.Condition()

        self.swarm_memory_interface = SwarmMemoryInterface(self.executor_interface)

        self.execution_group = {}

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
        self.assign_msg_handler(str(MessageTypes.EXECUTION_GROUP_TASK), self.handle_execution_group_task_message)
        self.assign_msg_handler(str(MessageTypes.START_TASK_EXECUTION), self.handle_start_task_execution_message)
        self.assign_msg_handler(str(MessageTypes.EXECUTION_GROUP_TEARDOWN), self.handle_execution_group_teardown_message)
        self.assign_msg_handler(str(MessageTypes.DELETE_FROM_SWARM_MEMORY), self.handle_delete_from_swarm_memory_message)
        self.assign_msg_handler(str(MessageTypes.TASK_OUTPUT), self.handle_task_output_message)

    def startup(self):
        NetworkNode.startup(self)
        self.start_task_executor()

    def teardown(self):
        NetworkNode.teardown(self)
        self.task_queue_has_values.set()

    def start_task_executor(self):
        thread = threading.Thread(target=self.task_executor_loop)
        thread.start()

    def add_task_execution_listener(self, new_listener):
        self.task_execution_listeners.append(new_listener)

    def get_assigned_task(self):
        return self.assigned_task

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

    def get_next_task_to_execute(self):
        next_task = None
        task_queue = self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)
        while ((next_task is None) and (len(task_queue) > 0)):
            if len(task_queue) > 0:
                task_queue.sort(key=self.task_scheduling_algorithm)
                next_task_id = task_queue.pop(0)
                next_task = self.swarm_memory_interface.pop_from_swarm_memory(next_task_id)
            task_queue = self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)
        return next_task

    def get_task_bundle_queue(self):
        return self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)

    def get_task_queue(self):
        return self.swarm_memory_interface.get_ids_of_contents_of_type(SwarmTask.__name__)

    def teardown_execution_group(self):
        for bot_id in list(self.execution_group.keys()):
            if bot_id != self.get_id():
                self.send_directed_message(bot_id, MessageTypes.EXECUTION_GROUP_TEARDOWN, {}, False)
        self.execution_group = {}
        with self.execution_group_lock:
            self.execution_group_lock.notify_all()

    def received_all_required_task_outputs(self, execution_group):
        for _, task_info in execution_group.items():
            if task_info["OUTPUT"] is None:
                return False
        return True

    def wait_for_task_outputs(self, req_num_bots, index_in_bundle):
        if (req_num_bots > 1) and (index_in_bundle == 0):
            while not self.received_all_required_task_outputs(self.execution_group):
                with self.execution_group_lock:
                    check = self.execution_group_lock.wait(10)
                    if not check:
                        raise Exception("Did not receive task outputs in time.")

    def wait_for_execution_group_members(self, bundle_id, req_num_bots):
        self.execution_group_ledger[bundle_id] = self.get_id()
        self.max_execution_group_size = req_num_bots
        self.send_propagation_message(
            MessageTypes.EXECUTION_GROUP_CREATION,
            {"TASK_BUNDLE_ID": bundle_id, "OWNER_ID": self.get_id()}
        )
        while len(self.execution_group.keys()) < req_num_bots:
            with self.execution_group_lock:
                check = self.execution_group_lock.wait(10)
                if not check:
                    raise Exception("Could not form execution group within time limit.")

        for bot_id in list(self.execution_group.keys()):
            if bot_id != self.get_id():
                self.send_directed_message(
                    bot_id,
                    MessageTypes.START_TASK_EXECUTION,
                    {"EXECUTION_GROUP_INFO": self.execution_group},
                    False
                )

    def join_execution_group(self, bundle_id, task_type):
        if bundle_id not in self.execution_group_ledger:
            with self.execution_group_lock:
                check = self.execution_group_lock.wait(10)
                if not check:
                    raise Exception("Execution group was not created within time limit.")
        self.start_execution = False
        response = self.send_directed_message(
            self.execution_group_ledger[bundle_id],
            MessageTypes.REQUEST_JOIN_EXECUTION_GROUP,
            {"TASK_BUNDLE_ID": bundle_id, "TASK_TYPE": task_type},
            True
        )
        accepted = response.get_message_payload()["ACCEPTANCE_STATUS"]
        if not accepted:
            raise Exception("ERROR: Not able to join execution group.")

        listener_id = self.execution_group_ledger[bundle_id]

        if not self.start_execution:
            with self.execution_group_lock:
                check = self.execution_group_lock.wait(10)
                if not check:
                    raise Exception("Did not receive task start signal within time limit.")

        return listener_id

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

        self.execution_group_ledger[task_bundle_id] = owner_id
        with self.execution_group_lock:
            self.execution_group_lock.notify_all()

    def handle_request_join_execution_group_message(self, message):
        acceptance_status = False
        if len(self.execution_group.keys()) < self.max_execution_group_size:
            acceptance_status = True
        self.respond_to_message(message, {"ACCEPTANCE_STATUS": acceptance_status})
        self.execution_group[message.get_sender_id()] = {
            "TASK_TYPE": message.get_message_payload()["TASK_TYPE"],
            "OUTPUT": None
        }
        with self.execution_group_lock:
            self.execution_group_lock.notify_all()

    def handle_execution_group_task_message(self, message):
        msg_payload = message.get_message_payload()
        task = msg_payload["TASK"]
        execution_group = msg_payload["EXECUTION_GROUP"]

        self.assigned_task = task
        self.execution_group = execution_group
        with self.execution_group_lock:
            self.execution_group_lock.notify_all()

    def handle_start_task_execution_message(self, message):
        self.execution_group = message.get_message_payload()["EXECUTION_GROUP_INFO"]
        self.start_execution = True
        with self.execution_group_lock:
            self.execution_group_lock.notify_all()

    def handle_execution_group_teardown_message(self, message):
        self.execution_group = {}
        with self.execution_group_lock:
            self.execution_group_lock.notify_all()

    def handle_task_output_message(self, message):
        msg_payload = message.get_message_payload()
        task_type = msg_payload["TASK_TYPE"]
        task_output = msg_payload["TASK_OUTPUT"][task_type][0]
        bot_id = message.get_sender_id()

        if bot_id in self.execution_group:
            self.execution_group[message.get_sender_id()]["OUTPUT"] = task_output
            with self.execution_group_lock:
                self.execution_group_lock.notify_all()

    def task_executor_loop(self):
        while (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
            self.task_queue_has_values.wait()

            if (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
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

                self.execution_group = {
                    self.get_id(): {
                        "TASK_TYPE": next_task.__class__.__name__,
                        "OUTPUT": None
                    }
                }

                if (req_num_bots > 1):
                    if index_in_bundle == 0:
                        self.wait_for_execution_group_members(bundle_id, req_num_bots)
                    else:
                        listener_id = self.join_execution_group(bundle_id, task_type)

                self.assigned_task = next_task

                self.task_execution_history.append(self.assigned_task)
                self.swarm_memory_interface.pop_from_swarm_memory(next_task_id)
                self.assigned_task.setup(self.executor_interface, self.execution_group)
                curr_execution = 0
                max_executions = self.max_task_executions
                while (not self.assigned_task.is_complete()) and (curr_execution < max_executions):
                    self.assigned_task.execute_task()
                    curr_execution += 1

                self.execution_group[self.get_id()]["OUTPUT"] = self.assigned_task.get_task_output()

                if listener_id is not None:
                    self.wait_for_task_outputs(req_num_bots, index_in_bundle)

                    final_output = {}
                    for bot_id, task_info in self.execution_group.items():
                        task_type = task_info["TASK_TYPE"]
                        task_output = task_info["OUTPUT"]
                        if task_type not in final_output:
                            final_output[task_type] = []

                        final_output[task_type].append(task_output)
                    self.send_directed_message(
                        listener_id,
                        MessageTypes.TASK_OUTPUT,
                        {"TASK_ID": next_task_id, "TASK_OUTPUT": final_output, "TASK_TYPE": task_type},
                        False
                    )

                self.assigned_task = None

                self._notify_process_state(False)
