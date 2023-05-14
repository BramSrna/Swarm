import threading
import os

from network_manager.network_node.network_node import NetworkNode
from swarm.executor_interface import ExecutorInterface
from swarm.message_types import MessageTypes
from swarm.swarm_task.task_scheduling_algorithms import simple_task_sort
from swarm.swarm_memory.swarm_memory_interface import SwarmMemoryInterface
from network_manager.network_node.network_node_message_types import NetworkNodeMessageTypes
from swarm.swarm_task.task_executor_pool import TaskExecutorPool


class SwarmBot(NetworkNode):
    def __init__(
            self,
            additional_config_path: str = os.path.join(os.path.dirname(__file__), "./default_swarm_bot_config.yml"),
            additional_config_dict: dict = None
            ):
        super().__init__(
            additional_config_path=additional_config_path,
            additional_config_dict=additional_config_dict
        )

        self.response_locks = {}
        self.msg_intermediaries = {}

        task_scheduling_algorithms = {
            "SimpleTaskSort": simple_task_sort
        }

        self.max_num_task_executors = self.config["max_num_task_executors"]
        self.max_task_executions = self.config["max_task_executions"]
        self.task_scheduling_algorithm = task_scheduling_algorithms[self.config["task_scheduling_algorithm"]]
        self.swarm_memory_optimization_operation_threshold = self.config["swarm_memory_optimization_operation_threshold"]
        self.key_count_threshold = self.config["swarm_memory_key_count_threshold"]

        self.assign_msg_handler(
            str(NetworkNodeMessageTypes.REQUEST_CONNECTION),
            self.swarm_bot_handle_request_connection_message
        )
        self.assign_msg_handler(
            str(NetworkNodeMessageTypes.BOT_TEARDOWN),
            self.swarm_bot_handle_bot_teardown_message
        )

        self.assign_msg_handler(
            str(MessageTypes.NEW_SWARM_BOT_ID),
            self.swarm_bot_handle_new_swarm_bot_id_message
        )
        self.assign_msg_handler(
            str(MessageTypes.FORWARD_MESSAGE),
            self.swarm_bot_handle_forward_message_message
        )
        self.assign_msg_handler(
            str(MessageTypes.SYNC_INTERMEDIARIES),
            self.swarm_bot_handle_sync_intermediaries_message
        )
        self.assign_msg_handler(
            str(MessageTypes.REQUEST_PATH_TO_BOT),
            self.swarm_bot_handle_request_path_to_bot
        )
        self.assign_msg_handler(
            str(MessageTypes.MSG_RESPONSE),
            self.swarm_bot_handle_msg_response_message
        )

        self.swarm_memory_interface = SwarmMemoryInterface(
            ExecutorInterface(self),
            self.swarm_memory_optimization_operation_threshold,
            self.key_count_threshold
        )

        self.task_executor_pool = TaskExecutorPool(
            ExecutorInterface(self),
            self.max_num_task_executors,
            self.max_task_executions,
            self.task_scheduling_algorithm
        )

    def get_simulator_for_task(self, task_type):
        simulators = self.read_from_swarm_memory("TASK_SIMULATORS")
        print(simulators)
        return self.read_from_swarm_memory("TASK_SIMULATORS/" + task_type.__name__)

    def get_known_bot_ids(self):
        return list(self.msg_intermediaries.keys())

    def get_local_swarm_memory_contents(self):
        return self.swarm_memory_interface.get_local_swarm_memory_contents()

    def add_path_watcher(self, path_to_watch, method_to_call):
        return self.swarm_memory_interface.add_path_watcher(path_to_watch, method_to_call)

    def set_task_executor_status(self, new_status):
        self.task_executor_pool.set_task_executor_status(new_status)

    def get_task_queue(self):
        task_queue = self.read_from_swarm_memory("TASK_QUEUE")
        if task_queue is None:
            return []
        else:
            return list(task_queue.keys())

    def teardown(self):
        self.swarm_memory_interface.teardown()
        self.send_propagation_message(NetworkNodeMessageTypes.BOT_TEARDOWN, {"BOT_ID": self.get_id()})
        self.task_executor_pool.teardown()
        self.set_message_inbox_status(False)
        self.set_message_outbox_status(False)

        self.unassign_msg_handler(
            str(NetworkNodeMessageTypes.REQUEST_CONNECTION),
            self.swarm_bot_handle_request_connection_message
        )
        self.unassign_msg_handler(
            str(NetworkNodeMessageTypes.BOT_TEARDOWN),
            self.swarm_bot_handle_bot_teardown_message
        )

        self.unassign_msg_handler(
            str(MessageTypes.NEW_SWARM_BOT_ID),
            self.swarm_bot_handle_new_swarm_bot_id_message
        )
        self.unassign_msg_handler(
            str(MessageTypes.FORWARD_MESSAGE),
            self.swarm_bot_handle_forward_message_message
        )
        self.unassign_msg_handler(
            str(MessageTypes.SYNC_INTERMEDIARIES),
            self.swarm_bot_handle_sync_intermediaries_message
        )
        self.unassign_msg_handler(
            str(MessageTypes.REQUEST_PATH_TO_BOT),
            self.swarm_bot_handle_request_path_to_bot
        )
        self.unassign_msg_handler(
            str(MessageTypes.MSG_RESPONSE),
            self.swarm_bot_handle_msg_response_message
        )

        self.wait_until_idle()
        NetworkNode.teardown(self)

    def receive_task_bundle(self, new_task_bundle, listener_bot_id=None):
        tasks = new_task_bundle.get_tasks()
        for i in range(len(tasks)):
            curr_task = tasks[i]
            self.write_to_swarm_memory(
                "TASK_QUEUE/" + str(curr_task.get_id()),
                {
                    "TASK": curr_task,
                    "PARENT_BUNDLE_ID": new_task_bundle.get_id(),
                    "REQ_NUM_BOTS": new_task_bundle.get_req_num_bots(),
                    "INDEX_IN_BUNDLE": i,
                    "LISTENER_ID": listener_bot_id
                }
            )
        return True

    def write_to_swarm_memory(self, path_to_create, value):
        return self.swarm_memory_interface.write(path_to_create, value)

    def read_from_swarm_memory(self, path_to_read):
        return self.swarm_memory_interface.read(path_to_read)

    def delete_from_swarm_memory(self, path_to_delete):
        return self.swarm_memory_interface.delete(path_to_delete)

    def get_task_execution_history(self):
        return self.task_executor_pool.get_task_execution_history()

    def receive_message(self, message):
        payload = message.get_message_payload()

        target_bot_id = self.get_id()
        if "TARGET_BOT_ID" in payload:
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
        return NetworkNode.send_propagation_message(self, message_type, message_payload)

    def send_directed_message(self, target_bot_id, message_type, message_payload):
        message_payload["TARGET_BOT_ID"] = target_bot_id
        message_payload["ORIGINAL_SENDER_ID"] = self.get_id()

        first_intermediary_id = None
        if target_bot_id == self.get_id():
            raise Exception("ERROR: {} tried to send message to self: {}".format(
                self.get_id(),
                target_bot_id
            ))
        elif target_bot_id in self.msg_channels:
            first_intermediary_id = target_bot_id
        elif target_bot_id in self.msg_intermediaries:
            first_intermediary_id = self.msg_intermediaries[target_bot_id]["INTERMEDIARY_ID"]
        elif target_bot_id in self.torndown_nodes:
            self.logger.warning("WARNING: {} tried to send message to torn down bot: {}".format(
                self.get_id(),
                target_bot_id
            ))
            return None
        else:
            raise Exception("ERROR: {} tried to send message to unknown bot: {}".format(
                self.get_id(),
                target_bot_id
            ))

        return NetworkNode.send_directed_message(self, first_intermediary_id, message_type, message_payload)

    def send_sync_directed_message(self, target_bot_id, message_type, message_payload):
        msg_id = self.send_directed_message(target_bot_id, message_type, message_payload)
        if msg_id is None:
            return None

        self.response_locks[msg_id] = {
            "LOCK": threading.Condition(),
            "RESPONSE": None,
            "TARGET_BOT_ID": target_bot_id
        }

        with self.response_locks[msg_id]["LOCK"]:
            check = self.response_locks[msg_id]["LOCK"].wait(timeout=10)
            if check:
                return self.response_locks.pop(msg_id)["RESPONSE"]
            else:
                if target_bot_id in self.torndown_nodes:
                    return None
                else:
                    raise Exception("ERROR: Did not receive message response within time limit. Message ID: {}".format(msg_id))

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
                if (potential_intermediary_id not in self.msg_intermediaries):
                    if (target_bot_id not in self.msg_intermediaries):
                        self.send_propagation_message(MessageTypes.REQUEST_PATH_TO_BOT, {"BOT_ID": target_bot_id})
                    return False
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

    def get_id_with_shortest_path_from_list(self, list_of_ids):
        if self.get_id() in list_of_ids:
            return self.get_id()

        if len(list_of_ids) == 0:
            return None

        shortest_length = self.get_num_jumps_to(list_of_ids[0])
        id_with_shortest_path = list_of_ids[0]

        for id in list_of_ids:
            num_jumps = self.get_num_jumps_to(id)
            if num_jumps < shortest_length:
                shortest_length = num_jumps
                id_with_shortest_path = id

        return id_with_shortest_path

    def swarm_bot_handle_new_swarm_bot_id_message(self, message):
        msg_payload = message.get_message_payload()
        intermediary = msg_payload["MSG_INTERMEDIARY"]
        new_bot_id = msg_payload["NEW_BOT_ID"]
        num_jumps = msg_payload["NUM_JUMPS"]
        self.save_msg_intermediary(new_bot_id, intermediary, num_jumps)

    def swarm_bot_handle_forward_message_message(self, message):
        original_message = message.get_message_payload()["ORIGINAL_MESSAGE"]
        target_bot_id = original_message.get_message_payload()["TARGET_BOT_ID"]
        if target_bot_id == self.get_id():
            original_message.set_sender_id(message.get_sender_id())
            self.receive_message(original_message)
        else:
            self.send_directed_message(target_bot_id, MessageTypes.FORWARD_MESSAGE, {"ORIGINAL_MESSAGE": original_message})

    def swarm_bot_handle_request_connection_message(self, message):
        new_id = message.get_sender_id()
        self.save_msg_intermediary(new_id, new_id, 1)
        self.send_propagation_message(
            MessageTypes.NEW_SWARM_BOT_ID,
            {"MSG_INTERMEDIARY": self.get_id(), "NEW_BOT_ID": new_id, "NUM_JUMPS": 1}
        )
        self.send_directed_message(
            new_id,
            MessageTypes.SYNC_INTERMEDIARIES,
            {"INTERMEDIARY_LIST": self.msg_intermediaries}
        )

    def swarm_bot_handle_sync_intermediaries_message(self, message):
        intermediary_list = message.get_message_payload()["INTERMEDIARY_LIST"]
        sender_id = message.get_sender_id()
        for intermediary_id in intermediary_list:
            self.save_msg_intermediary(intermediary_id, sender_id, intermediary_list[intermediary_id]["NUM_JUMPS"])
        bot_ids = list(self.msg_intermediaries.keys())
        for bot_id in bot_ids:
            self.send_propagation_message(
                MessageTypes.NEW_SWARM_BOT_ID,
                {
                    "MSG_INTERMEDIARY": self.get_id(),
                    "NEW_BOT_ID": bot_id,
                    "NUM_JUMPS": self.msg_intermediaries[bot_id]["NUM_JUMPS"]
                }
            )

    def swarm_bot_handle_bot_teardown_message(self, message):
        bot_to_remove = message.get_message_payload()["BOT_ID"]

        if bot_to_remove in self.msg_intermediaries:
            self.msg_intermediaries.pop(bot_to_remove)

        needs_new_path = []
        for target_bot_id, intermediary_info in self.msg_intermediaries.items():
            if intermediary_info["INTERMEDIARY_ID"] == bot_to_remove:
                needs_new_path.append(target_bot_id)
        for bot_id in needs_new_path:
            self.msg_intermediaries.pop(bot_id)

        for bot_id in needs_new_path:
            self.send_propagation_message(MessageTypes.REQUEST_PATH_TO_BOT, {"BOT_ID": bot_id})

        for msg_id in self.response_locks:
            if self.response_locks[msg_id]["TARGET_BOT_ID"] == bot_to_remove:
                self.response_locks[msg_id]["RESPONSE"] = None
                with self.response_locks[msg_id]["LOCK"]:
                    self.response_locks[msg_id]["LOCK"].notify_all()

    def swarm_bot_handle_request_path_to_bot(self, message):
        bot_id = message.get_message_payload()["BOT_ID"]
        if bot_id in self.msg_channels:
            self.send_directed_message(
                message.get_sender_id(),
                MessageTypes.NEW_SWARM_BOT_ID,
                {"MSG_INTERMEDIARY": self.get_id(), "NEW_BOT_ID": bot_id, "NUM_JUMPS": 1}
            )

    def swarm_bot_handle_msg_response_message(self, message):
        message_payload = message.get_message_payload()
        original_message_id = message_payload["ORIGINAL_MESSAGE_ID"]
        if original_message_id not in self.response_locks:
            self.logger.warning("WARNING: Received message response for message that was never sent: {}".format(
                message.get_message_payload()
            ))
        else:
            self.response_locks[original_message_id]["RESPONSE"] = message
            with self.response_locks[original_message_id]["LOCK"]:
                self.response_locks[original_message_id]["LOCK"].notify_all()
