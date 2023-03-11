import threading
import os
import copy

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

        self.response_locks = {}
        self.msg_intermediaries = {}

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
        self.assign_msg_handler(str(MessageTypes.RESPOND_TO_READ), self.handle_respond_to_read_message)
        self.assign_msg_handler(str(MessageTypes.NEW_SWARM_BOT_ID), self.handle_new_swarm_bot_id_message)
        self.assign_msg_handler(str(MessageTypes.FORWARD_MESSAGE), self.handle_forward_message_message)
        self.assign_msg_handler(str(MessageTypes.SYNC_INTERMEDIARIES), self.handle_sync_intermediaries_message)
        self.assign_msg_handler(str(MessageTypes.BOT_TEARDOWN), self.handle_bot_teardown_message)
        self.assign_msg_handler(str(MessageTypes.REQUEST_PATH_TO_BOT), self.handle_request_path_to_bot)
        self.assign_msg_handler(str(MessageTypes.MSG_RESPONSE), self.handle_msg_response_message)
        self.assign_msg_handler(str(MessageTypes.UPDATE_SWARM_MEMORY_VALUE), self.handle_update_swarm_memory_value)

    def startup(self):
        NetworkNode.startup(self)
        self.start_task_executor()

    def teardown(self):
        self.send_propagation_message(MessageTypes.BOT_TEARDOWN, {"BOT_ID": self.get_id()})
        self.set_task_executor_status(False)
        self.set_message_inbox_status(False)
        self.set_message_outbox_status(False)
        self.wait_until_idle()
        NetworkNode.teardown(self)

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

    def update_swarm_memory(self, key_to_update, new_value):
        self.swarm_memory_interface.update_swarm_memory(key_to_update, new_value)

    def get_task_execution_history(self):
        return self.task_execution_history

    def set_task_executor_status(self, new_status):
        if new_status and self.run_task_executor.is_set():
            self.run_task_executor.clear()
            self.start_task_executor()
        elif (not new_status) and (not self.run_task_executor.is_set()):
            self.run_task_executor.set()
            self.task_queue_has_values.set()

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

    def receive_message(self, message):
        target_bot_id = message.get_message_payload()["TARGET_BOT_ID"]
        final_message = message
        if target_bot_id != self.get_id():
            if target_bot_id not in self.msg_intermediaries:
                self.logger.warning(
                    "ERROR: {} tried to forward message to bot with no known path: {}".format(
                        self.get_id(),
                        target_bot_id
                    )
                )
                return None

            final_message = self.message_wrapper_type(
                self._generate_message_id(),
                message.get_sender_id(),
                self.msg_intermediaries[target_bot_id]["INTERMEDIARY_ID"],
                MessageTypes.FORWARD_MESSAGE,
                {"ORIGINAL_MESSAGE": message},
                False
            )

        return NetworkNode.receive_message(self, final_message)

    def connect_to_network_node(self, new_network_node):
        self.save_msg_intermediary(new_network_node.get_id(), new_network_node.get_id(), 1)
        NetworkNode.connect_to_network_node(self, new_network_node)

    def get_msg_intermediaries(self):
        return self.msg_intermediaries

    def send_propagation_message(self, message_type, message_payload):
        msg_ids = []
        for bot_id in list(self.msg_intermediaries.keys()):
            curr_id = self.send_directed_message(bot_id, message_type, copy.deepcopy(message_payload))
            msg_ids.append(curr_id)
        return msg_ids

    def send_directed_message(self, target_bot_id, message_type, message_payload):
        message_payload["TARGET_BOT_ID"] = target_bot_id
        message_payload["ORIGINAL_SENDER_ID"] = self.get_id()

        if target_bot_id not in self.msg_intermediaries:
            raise Exception("ERROR: {} tried to send message to bot with no known path: {}".format(
                self.get_id(),
                target_bot_id
            ))
        first_intermediary_id = self.msg_intermediaries[target_bot_id]["INTERMEDIARY_ID"]

        return NetworkNode.send_directed_message(self, first_intermediary_id, message_type, message_payload)

    def send_sync_directed_message(self, target_bot_id, message_type, message_payload):
        msg_id = self.send_directed_message(target_bot_id, message_type, message_payload)

        self.response_locks[msg_id] = {
            "LOCK": threading.Condition(),
            "RESPONSE": None
        }
        with self.response_locks[msg_id]["LOCK"]:
            check = self.response_locks[msg_id]["LOCK"].wait(timeout=10)
            if not check:
                raise Exception("ERROR: Did not receive message response within time limit. Message ID: {}".format(msg_id))
        return self.response_locks[msg_id]["RESPONSE"]

    def respond_to_message(self, message, message_payload):
        message_payload["ORIGINAL_MESSAGE_ID"] = message.get_id()
        target_bot_id = message.get_message_payload()["ORIGINAL_SENDER_ID"]
        self.send_directed_message(target_bot_id, MessageTypes.MSG_RESPONSE, message_payload)

    def save_msg_intermediary(self, target_bot_id, intermediary_id, num_jumps):
        if target_bot_id == self.get_id():
            return False

        potential_intermediary_id = intermediary_id
        if target_bot_id != potential_intermediary_id:
            num_jumps += 1
            while potential_intermediary_id not in self.msg_channels:
                if potential_intermediary_id not in self.msg_intermediaries:
                    raise Exception("ERROR: Could not find directly connected intermediary.")
                potential_intermediary_id = self.msg_intermediaries[potential_intermediary_id]["INTERMEDIARY_ID"]
                num_jumps += 1

        if (target_bot_id not in self.msg_intermediaries) or (num_jumps < self.msg_intermediaries[target_bot_id]["NUM_JUMPS"]):
            self.msg_intermediaries[target_bot_id] = {
                "INTERMEDIARY_ID": potential_intermediary_id,
                "NUM_JUMPS": num_jumps
            }

    def get_num_jumps_to(self, target_bot_id):
        if target_bot_id in self.msg_intermediaries:
            return self.msg_intermediaries[target_bot_id]["NUM_JUMPS"]
        return None

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

    def handle_respond_to_read_message(self, message):
        msg_payload = message.get_message_payload()
        object_key = msg_payload["OBJECT_KEY"]
        object_value = msg_payload["OBJECT_VALUE"]
        data_type = msg_payload["DATA_TYPE"]
        self.write_to_swarm_memory(object_key, object_value, data_type)
        if data_type == SwarmTask.__name__:
            self.task_queue_has_values.set()

    def handle_new_swarm_bot_id_message(self, message):
        msg_payload = message.get_message_payload()
        intermediary = msg_payload["MSG_INTERMEDIARY"]
        new_bot_id = msg_payload["NEW_BOT_ID"]
        num_jumps = msg_payload["NUM_JUMPS"]
        self.save_msg_intermediary(new_bot_id, intermediary, num_jumps)

    def handle_forward_message_message(self, message):
        original_message = message.get_message_payload()["ORIGINAL_MESSAGE"]
        target_bot_id = original_message.get_message_payload()["TARGET_BOT_ID"]
        if target_bot_id == self.get_id():
            original_message.set_sender_id(message.get_sender_id())
            self.receive_message(original_message)
        else:
            self.send_directed_message(target_bot_id, MessageTypes.FORWARD_MESSAGE, {"ORIGINAL_MESSAGE": original_message})

    def handle_request_connection_message(self, message):
        new_id = message.get_sender_id()
        if new_id not in self.msg_channels:
            self.save_msg_intermediary(new_id, new_id, 1)
            NetworkNode.handle_request_connection_message(self, message)
            self.send_propagation_message(
                MessageTypes.NEW_SWARM_BOT_ID,
                {"MSG_INTERMEDIARY": self.get_id(), "NEW_BOT_ID": new_id, "NUM_JUMPS": 1}
            )
            self.send_directed_message(
                new_id,
                MessageTypes.SYNC_INTERMEDIARIES,
                {"INTERMEDIARY_LIST": self.msg_intermediaries}
            )

    def handle_accept_connection_request_message(self, message):
        NetworkNode.handle_accept_connection_request_message(self, message)

    def handle_sync_intermediaries_message(self, message):
        intermediary_list = message.get_message_payload()["INTERMEDIARY_LIST"]
        sender_id = message.get_sender_id()
        for intermediary_id in intermediary_list:
            self.save_msg_intermediary(intermediary_id, sender_id, intermediary_list[intermediary_id]["NUM_JUMPS"])
        for bot_id in self.msg_intermediaries:
            self.send_propagation_message(
                MessageTypes.NEW_SWARM_BOT_ID,
                {
                    "MSG_INTERMEDIARY": self.get_id(),
                    "NEW_BOT_ID": bot_id,
                    "NUM_JUMPS": self.msg_intermediaries[bot_id]["NUM_JUMPS"]
                }
            )

    def handle_bot_teardown_message(self, message):
        bot_to_remove = message.get_message_payload()["BOT_ID"]

        if bot_to_remove in self.msg_intermediaries:
            self.msg_intermediaries.pop(bot_to_remove)
        if bot_to_remove in self.msg_channels:
            self.msg_channels.pop(bot_to_remove)

        needs_new_path = []
        for target_bot_id, intermediary_info in self.msg_intermediaries.items():
            if intermediary_info["INTERMEDIARY_ID"] == bot_to_remove:
                needs_new_path.append(target_bot_id)
        for bot_id in needs_new_path:
            self.msg_intermediaries.pop(bot_id)

        for bot_id in needs_new_path:
            self.send_propagation_message(MessageTypes.REQUEST_PATH_TO_BOT, {"BOT_ID": bot_id})

    def handle_request_path_to_bot(self, message):
        bot_id = message.get_message_payload()["BOT_ID"]
        if bot_id in self.msg_channels:
            self.send_directed_message(
                message.get_sender_id(),
                MessageTypes.NEW_SWARM_BOT_ID,
                {"MSG_INTERMEDIARY": self.get_id(), "NEW_BOT_ID": bot_id, "NUM_JUMPS": 1}
            )

    def handle_msg_response_message(self, message):
        message_payload = message.get_message_payload()
        original_message_id = message_payload["ORIGINAL_MESSAGE_ID"]
        if original_message_id not in self.response_locks:
            raise Exception("ERROR: Received message response for message that was never sent: {}".format(
                message.get_message_payload()
            ))
        self.response_locks[original_message_id]["RESPONSE"] = message
        with self.response_locks[original_message_id]["LOCK"]:
            self.response_locks[original_message_id]["LOCK"].notify_all()

    def handle_update_swarm_memory_value(self, message):
        message_payload = message.get_message_payload()
        key_to_update = message_payload["KEY_TO_UPDATE"]
        new_value = message_payload["NEW_VALUE"]
        self.update_swarm_memory(key_to_update, new_value)

    def task_executor_loop(self):
        while (not self.run_node.is_set()) and (not self.run_task_executor.is_set()):
            self.task_queue_has_values.wait()

            if (not self.run_node.is_set()) and (not self.run_task_executor.is_set()) and \
                    (len(self.task_executors.keys()) < self.max_num_task_executors):
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
