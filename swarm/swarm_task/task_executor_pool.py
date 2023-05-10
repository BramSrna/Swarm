import threading

from swarm.swarm_task.swarm_task_message_types import SwarmTaskMessageTypes
from swarm.swarm_task.task_execution_controller import TaskExecutionController


class TaskExecutorPool(object):
    def __init__(self, executor_interface, max_num_task_executors, max_task_executions, task_scheduling_algorithm):
        self.executor_interface = executor_interface
        self.max_num_task_executors = max_num_task_executors
        self.max_task_executions = max_task_executions
        self.task_scheduling_algorithm = task_scheduling_algorithm

        self.run_task_executor = True
        self.task_executors = []
        self.idle_executors = []
        self.idle_executors_lock = threading.Lock()
        self.task_queue = []
        self.task_queue_lock = threading.Lock()
        self.execution_group_ledger = {}

        self.task_execution_history = []

        self.executor_interface.add_path_watcher("TASK_QUEUE", self.task_queue_monitor)

        self.executor_interface.assign_msg_handler(
            str(SwarmTaskMessageTypes.EXECUTION_GROUP_CREATION),
            self.task_executor_pool_handle_execution_group_creation_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmTaskMessageTypes.REQUEST_JOIN_EXECUTION_GROUP),
            self.task_executor_pool_handle_request_join_execution_group_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmTaskMessageTypes.EXECUTION_GROUP_DELETION),
            self.task_executor_pool_handle_execution_group_deletion_message
        )

        for _ in range(self.max_num_task_executors):
            task_executor = TaskExecutionController(self, self.executor_interface, self.max_task_executions)
            self.task_executors.append(task_executor)

    def teardown(self):
        self.set_task_executor_status(False)

        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.EXECUTION_GROUP_CREATION),
            self.task_executor_pool_handle_execution_group_creation_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.REQUEST_JOIN_EXECUTION_GROUP),
            self.task_executor_pool_handle_request_join_execution_group_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmTaskMessageTypes.EXECUTION_GROUP_DELETION),
            self.task_executor_pool_handle_execution_group_deletion_message
        )

    def add_new_execution_group_leader(self, task_bundle_id, owner_id, time_created):
        if (task_bundle_id in self.execution_group_ledger):
            current_time_created = self.execution_group_ledger[task_bundle_id]["TIME_CREATED"]
            if (time_created > current_time_created):
                return False
            if (time_created == current_time_created) and (self.executor_interface.get_id() < owner_id):
                return False

        self.execution_group_ledger[task_bundle_id] = {
            "OWNER": owner_id,
            "TIME_CREATED": time_created
        }
        return True

    def get_task_execution_history(self):
        return self.task_execution_history

    def set_task_executor_status(self, new_status):
        self.run_task_executor = new_status
        if self.run_task_executor:
            num_idle_executors = len(self.idle_executors)
            while num_idle_executors > 0:
                self.run_task_scheduler()
                num_idle_executors -= 1

    def get_execution_group_ledger(self):
        return self.execution_group_ledger

    def notify_idle(self, task_executor):
        task = task_executor.get_task()
        if (task is not None):
            if (task_executor.get_completion_status()):
                self.task_execution_history.append(task)
            self.executor_interface._notify_process_state(False)
        with self.idle_executors_lock:
            self.idle_executors.append(task_executor)
        self.run_task_scheduler()

    def task_queue_monitor(self, snapshot):
        with self.task_queue_lock:
            new_task_ids = snapshot
            for task_id in new_task_ids:
                if task_id not in self.task_queue:
                    self.task_queue.append(task_id)
        self.run_task_scheduler()

    def get_idle_task_executor(self):
        with self.idle_executors_lock:
            task_executor = None
            if len(self.idle_executors) > 0:
                task_executor = self.idle_executors.pop(0)
            return task_executor

    def get_info_of_next_task_to_execute(self):
        with self.task_queue_lock:
            task_info = None
            if len(self.task_queue) > 0:
                self.task_queue.sort(key=self.task_scheduling_algorithm)

                while ((task_info is None) and (len(self.task_queue) > 0)):
                    next_task_id = self.task_queue.pop(0)
                    task_info = self.executor_interface.read_from_swarm_memory("TASK_QUEUE/" + str(next_task_id))
            return task_info

    def run_task_scheduler(self):
        if not self.run_task_executor:
            return False

        task_executor = self.get_idle_task_executor()
        if (not self.run_task_executor) or (task_executor is None):
            return False

        task_info = self.get_info_of_next_task_to_execute()
        if (not self.run_task_executor) or (task_info is None):
            with self.idle_executors_lock:
                self.idle_executors.append(task_executor)
            return False

        task = task_info["TASK"]
        req_num_bots = task_info["REQ_NUM_BOTS"]
        bundle_id = task_info["PARENT_BUNDLE_ID"]
        index_in_bundle = task_info["INDEX_IN_BUNDLE"]
        listener_id = task_info["LISTENER_ID"]

        self.executor_interface.delete_from_swarm_memory("TASK_QUEUE/" + str(task.get_id()))
        self.executor_interface._notify_process_state(True)
        task_executor.execute_task(task, bundle_id, index_in_bundle, listener_id, req_num_bots)

    def task_executor_pool_handle_execution_group_creation_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]
        owner_id = msg_payload["OWNER_ID"]
        time_created = msg_payload["CREATION_TIME"]

        self.add_new_execution_group_leader(task_bundle_id, owner_id, time_created)

    def task_executor_pool_handle_request_join_execution_group_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]

        for task_executor in self.task_executors:
            if task_executor.get_bundle_id() == task_bundle_id:
                return None

        self.executor_interface.respond_to_message(message, {
            "REQUEST_STATUS": False
        })

    def task_executor_pool_handle_execution_group_deletion_message(self, message):
        msg_payload = message.get_message_payload()
        task_bundle_id = msg_payload["TASK_BUNDLE_ID"]

        if task_bundle_id in self.execution_group_ledger:
            self.execution_group_ledger.pop(task_bundle_id)
