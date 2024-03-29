import time
import threading

from enum import Enum

from swarm.swarm_task.swarm_task_message_types import SwarmTaskMessageTypes


class TaskStates(Enum):
    SETUP = 1
    EXECUTION = 2
    TASK_OUTPUT = 3
    TEARDOWN = 4


class TaskExecutionController(object):
    def __init__(
            self,
            task_executor_pool,
            executor_interface,
            max_task_executions):
        self.task_executor_pool = task_executor_pool
        self.executor_interface = executor_interface
        self.max_task_executions = max_task_executions

        self.execution_group_lock = threading.Lock()

        self.task = None

        self.is_idle = True

        self.task_executor_pool.notify_idle(self)

    def execute_task(self, new_task, bundle_id, index_in_bundle, listener_id, req_num_bots):
        if self.is_idle:
            self.is_idle = False

            self.task = new_task
            self.bundle_id = bundle_id
            self.index_in_bundle = index_in_bundle
            self.task_type = self.task.__class__.__name__
            self.listener_id = listener_id
            self.task_id = new_task.get_id()
            self.req_num_bots = req_num_bots
            self.execution_group_owner_id = None
            self.task_completed = False
            self.execution_group = {}

            self.add_execution_group_member(self.executor_interface.get_id(), self.task_type, self.task_id)

            self.executor_interface.assign_msg_handler(
                str(SwarmTaskMessageTypes.EXECUTION_GROUP_CREATION),
                self.task_execution_controller_handle_execution_group_creation_message
            )
            self.executor_interface.assign_msg_handler(
                str(SwarmTaskMessageTypes.REQUEST_JOIN_EXECUTION_GROUP),
                self.task_execution_controller_handle_request_join_execution_group_message
            )
            self.executor_interface.assign_msg_handler(
                str(SwarmTaskMessageTypes.START_TASK_EXECUTION),
                self.task_execution_controller_handle_start_task_execution_message
            )
            self.executor_interface.assign_msg_handler(
                str(SwarmTaskMessageTypes.TASK_OUTPUT),
                self.task_execution_controller_handle_task_output_message
            )

            self.start_task_execution_process()

    def get_completion_status(self):
        return self.curr_state == TaskStates.TEARDOWN

    def get_bundle_id(self):
        return self.bundle_id

    def get_task(self):
        return self.task

    def start_task_execution_process(self):
        self.transition_state(TaskStates.SETUP)

    def setup_task(self):
        if self.index_in_bundle == 0:
            self.create_execution_group()
            if self.req_num_bots == 1:
                self.transition_state(TaskStates.EXECUTION)
        else:
            if self.bundle_id in self.task_executor_pool.get_execution_group_ledger():
                self.join_execution_group()

    def join_execution_group(self):
        response = self.executor_interface.send_sync_directed_message(
            self.task_executor_pool.get_execution_group_ledger()[self.bundle_id]["OWNER"],
            SwarmTaskMessageTypes.REQUEST_JOIN_EXECUTION_GROUP,
            {"TASK_BUNDLE_ID": self.bundle_id, "TASK_TYPE": self.task_type, "TASK_ID": self.task_id}
        )
        request_status = response.get_message_payload()["REQUEST_STATUS"]
        if request_status:
            self.listener_id = self.task_executor_pool.get_execution_group_ledger()[self.bundle_id]["OWNER"]
        else:
            self.transition_state(TaskStates.TEARDOWN)

    def received_all_required_task_outputs(self):
        for _, task_info in self.execution_group.items():
            if task_info["OUTPUT"] is None:
                return False
        return True

    def create_execution_group(self):
        time_created = time.time()
        exec_group_created = self.task_executor_pool.add_new_execution_group_leader(
            self.bundle_id,
            self.executor_interface.get_id(),
            time_created
        )
        self.execution_group_owner_id = self.task_executor_pool.get_execution_group_ledger()[self.bundle_id]["OWNER"]
        if not exec_group_created:
            self.transition_state(TaskStates.TEARDOWN)
        else:
            self.executor_interface.send_propagation_message(
                SwarmTaskMessageTypes.EXECUTION_GROUP_CREATION,
                {"TASK_BUNDLE_ID": self.bundle_id, "OWNER_ID": self.executor_interface.get_id(), "CREATION_TIME": time_created}
            )

    def execute_current_task(self):
        if self.task_executor_pool.get_execution_group_ledger()[self.bundle_id]["OWNER"] == self.executor_interface.get_id():
            for bot_id in list(self.execution_group.keys()):
                if bot_id != self.executor_interface.get_id():
                    self.executor_interface.send_directed_message(
                        bot_id,
                        SwarmTaskMessageTypes.START_TASK_EXECUTION,
                        {"EXECUTION_GROUP_INFO": self.execution_group, "TASK_BUNDLE_ID": self.bundle_id}
                    )

        self.task.setup(self.executor_interface, self.execution_group)
        curr_execution = 0
        max_executions = self.max_task_executions
        while (not self.task.is_complete()) and (curr_execution < max_executions):
            self.task.execute_task()
            curr_execution += 1

        with self.execution_group_lock:
            self.execution_group[self.executor_interface.get_id()]["OUTPUT"] = self.task.get_task_output()

        exec_group_leader_id = self.task_executor_pool.get_execution_group_ledger()[self.bundle_id]["OWNER"]
        am_exec_group_leader = exec_group_leader_id == self.executor_interface.get_id()
        if (am_exec_group_leader and self.received_all_required_task_outputs()) or (not am_exec_group_leader):
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
                SwarmTaskMessageTypes.TASK_OUTPUT,
                {
                    "TASK_ID": self.task_id,
                    "TASK_OUTPUT": final_output,
                    "TASK_TYPE": task_type,
                    "TASK_BUNDLE_ID": self.bundle_id
                }
            )

        self.task_completed = True

        self.transition_state(TaskStates.TEARDOWN)

    def teardown_execution_group(self):
        if self.task_completed:
            if (self.req_num_bots > 1) and (self.index_in_bundle == 0):
                self.executor_interface.send_propagation_message(
                    SwarmTaskMessageTypes.EXECUTION_GROUP_DELETION,
                    {"TASK_BUNDLE_ID": self.bundle_id}
                )
        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.EXECUTION_GROUP_CREATION),
            self.task_execution_controller_handle_execution_group_creation_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.REQUEST_JOIN_EXECUTION_GROUP),
            self.task_execution_controller_handle_request_join_execution_group_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.START_TASK_EXECUTION),
            self.task_execution_controller_handle_start_task_execution_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.TASK_OUTPUT),
            self.task_execution_controller_handle_task_output_message
        )

        self.is_idle = True
        print("DONE")
        self.task_executor_pool.notify_idle(self)

    def add_execution_group_member(self, owner_id, task_type, task_id):
        with self.execution_group_lock:
            for _, executor_info in self.execution_group.items():
                if executor_info["TASK_ID"] == task_id:
                    return False

            self.execution_group[owner_id] = {
                "TASK_ID": task_id,
                "TASK_TYPE": task_type,
                "OUTPUT": None
            }

            return True

    def task_execution_controller_handle_execution_group_creation_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]

        if task_bundle_id == self.bundle_id:
            new_execution_group_owner = self.task_executor_pool.get_execution_group_ledger()[task_bundle_id]["OWNER"]
            if (self.index_in_bundle == 0) and (new_execution_group_owner != self.execution_group_owner_id):
                self.execution_group_owner_id = new_execution_group_owner
                self.transition_state(TaskStates.TEARDOWN)
            elif (self.index_in_bundle != 0) and (new_execution_group_owner != self.execution_group_owner_id):
                self.execution_group_owner_id = new_execution_group_owner
                self.join_execution_group()

    def task_execution_controller_handle_request_join_execution_group_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        owner = msg_payload["ORIGINAL_SENDER_ID"]
        task_id = msg_payload["TASK_ID"]
        task_type = msg_payload["TASK_TYPE"]

        if task_bundle_id == self.bundle_id:
            added = self.add_execution_group_member(owner, task_type, task_id)
            if added:
                self.executor_interface.respond_to_message(message, {
                    "REQUEST_STATUS": True
                })

                if len(self.execution_group.keys()) >= self.req_num_bots:
                    self.transition_state(TaskStates.EXECUTION)
            else:
                self.executor_interface.respond_to_message(message, {
                    "REQUEST_STATUS": False
                })

    def task_execution_controller_handle_start_task_execution_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id == self.bundle_id:
            with self.execution_group_lock:
                self.execution_group = msg_payload["EXECUTION_GROUP_INFO"]
            self.transition_state(TaskStates.EXECUTION)

    def task_execution_controller_handle_task_output_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        if task_bundle_id == self.bundle_id:
            task_type = msg_payload["TASK_TYPE"]
            task_output = msg_payload["TASK_OUTPUT"][task_type][0]
            bot_id = message.get_sender_id()

            if bot_id in self.execution_group:
                with self.execution_group_lock:
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
