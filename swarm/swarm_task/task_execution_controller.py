from enum import Enum

from swarm.message_types import MessageTypes

class TaskStates(Enum):
    SETUP = 1
    EXECUTION = 2
    TASK_OUTPUT = 3
    TEARDOWN = 4

class TaskExecutionController(object):
    def __init__(self, bundle_id, index_in_bundle, task_type, listener_id, task, task_id, req_num_bots, executor_interface, max_task_executions):
        self.bundle_id = bundle_id
        self.index_in_bundle = index_in_bundle
        self.task_type = task_type
        self.listener_id = listener_id
        self.task = task
        self.task_id = task_id
        self.req_num_bots = req_num_bots
        self.executor_interface = executor_interface
        self.max_task_executions = max_task_executions

        self.execution_group = {
            self.executor_interface.get_id(): {
                "TASK_TYPE": self.task_type,
                "OUTPUT": None
            }
        }

        self.curr_state = None

    def start_task_execution_process(self):
        self.transition_state(TaskStates.SETUP)

    def setup_task(self):
        if self.index_in_bundle == 0:
            self.create_execution_group()
            if self.req_num_bots == 1:
                self.transition_state(TaskStates.EXECUTION)
        else:
            if self.bundle_id in self.executor_interface.get_execution_group_ledger():
                self.join_execution_group()

    def join_execution_group(self):
        response = self.executor_interface.send_directed_message(
            self.executor_interface.get_execution_group_ledger()[self.bundle_id],
            MessageTypes.REQUEST_JOIN_EXECUTION_GROUP,
            {"TASK_BUNDLE_ID": self.bundle_id, "TASK_TYPE": self.task_type},
            True
        )
        accepted = response.get_message_payload()["ACCEPTANCE_STATUS"]
        if not accepted:
            raise Exception("ERROR: Not able to join execution group.")
        self.listener_id = self.executor_interface.get_execution_group_ledger()[self.bundle_id]

    def received_all_required_task_outputs(self):
        for _, task_info in self.execution_group.items():
            if task_info["OUTPUT"] is None:
                return False
        return True

    def create_execution_group(self):
        self.executor_interface.add_new_execution_group_leader(self.bundle_id, self.executor_interface.get_id())
        self.executor_interface.send_propagation_message(
            MessageTypes.EXECUTION_GROUP_CREATION,
            {"TASK_BUNDLE_ID": self.bundle_id, "OWNER_ID": self.executor_interface.get_id()}
        )

    def execute_current_task(self):
        if self.executor_interface.get_execution_group_ledger()[self.bundle_id] == self.executor_interface.get_id():
            for bot_id in list(self.execution_group.keys()):
                if bot_id != self.executor_interface.get_id():
                    self.executor_interface.send_directed_message(
                        bot_id,
                        MessageTypes.START_TASK_EXECUTION,
                        {"EXECUTION_GROUP_INFO": self.execution_group, "TASK_BUNDLE_ID": self.bundle_id},
                        False
                    )

        self.task.setup(self.executor_interface, self.execution_group)
        curr_execution = 0
        max_executions = self.max_task_executions
        while (not self.task.is_complete()) and (curr_execution < max_executions):
            self.task.execute_task()
            curr_execution += 1

        self.execution_group[self.executor_interface.get_id()]["OUTPUT"] = self.task.get_task_output()

        if ((self.executor_interface.get_execution_group_ledger()[self.bundle_id] == self.executor_interface.get_id()) and self.received_all_required_task_outputs()) or (self.executor_interface.get_execution_group_ledger()[self.bundle_id] != self.executor_interface.get_id()):
            self.transition_state(TaskStates.TASK_OUTPUT)

    def send_task_output(self):
        if (self.listener_id is not None):
            final_output = {}
            for _, task_info in self.execution_group.items():
                task_type = task_info["TASK_TYPE"]
                task_output = task_info["OUTPUT"]
                if task_type not in final_output:
                    final_output[task_type] = []

                final_output[task_type].append(task_output)

            self.executor_interface.send_directed_message(
                self.listener_id,
                MessageTypes.TASK_OUTPUT,
                {"TASK_ID": self.task_id, "TASK_OUTPUT": final_output, "TASK_TYPE": task_type, "TASK_BUNDLE_ID": self.bundle_id},
                False
            )

        self.transition_state(TaskStates.TEARDOWN)

    def teardown_execution_group(self):
        if (self.req_num_bots > 1) and (self.index_in_bundle == 0):
            for bot_id in list(self.execution_group.keys()):
                if bot_id != self.executor_interface.get_id():
                    self.executor_interface.send_directed_message(bot_id, MessageTypes.EXECUTION_GROUP_TEARDOWN, {"TASK_BUNDLE_ID": self.bundle_id}, False)
            self.executor_interface.send_propagation_message(MessageTypes.EXECUTION_GROUP_DELETION, {"TASK_BUNDLE_ID": self.bundle_id})
        self.executor_interface.notify_task_completion(self.bundle_id)

    def handle_execution_group_creation_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id == self.bundle_id:
            self.join_execution_group()

    def handle_request_join_execution_group_message(self, message):
        msg_payload = message.get_message_payload()
        self.executor_interface.respond_to_message(message, {"ACCEPTANCE_STATUS": True})
        self.execution_group[message.get_sender_id()] = {
            "TASK_TYPE": msg_payload["TASK_TYPE"],
            "OUTPUT": None
        }

        if len(self.execution_group.keys()) >= self.req_num_bots:
            self.transition_state(TaskStates.EXECUTION)

    def handle_start_task_execution_message(self, message):
        msg_payload = message.get_message_payload()
        self.execution_group = msg_payload["EXECUTION_GROUP_INFO"]
        self.transition_state(TaskStates.EXECUTION)

    def handle_execution_group_teardown_message(self, message):
        self.execution_group = {}

    def handle_task_output_message(self, message):
        msg_payload = message.get_message_payload()
        task_type = msg_payload["TASK_TYPE"]
        task_output = msg_payload["TASK_OUTPUT"][task_type][0]
        bot_id = message.get_sender_id()

        if bot_id in self.execution_group:
            self.execution_group[message.get_sender_id()]["OUTPUT"] = task_output
            if self.received_all_required_task_outputs():
                self.transition_state(TaskStates.TASK_OUTPUT)

    def transition_state(self, new_state):
        self.curr_state = new_state
        if self.curr_state == TaskStates.SETUP:
            self.setup_task()
        elif self.curr_state == TaskStates.EXECUTION:
            self.execute_current_task()
        elif self.curr_state == TaskStates.TASK_OUTPUT:
            self.send_task_output()
        elif self.curr_state == TaskStates.TEARDOWN:
            self.teardown_execution_group()
        else:
            raise Exception("ERROR: Tried to transition to unknown state {}.".format(new_state))