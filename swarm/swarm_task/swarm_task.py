class SwarmTask(object):
    def __init__(self):
        self.id = id(self)

        self.executor_interface = None
        self.execution_group_info = None

    def get_req_num_bots(self):
        return self.req_num_bots

    def execute_task(self):
        raise Exception("ERROR: The execute_task method must be implemented by the concrete class.")

    def is_complete(self):
        raise Exception("ERROR: The is_complete method must be implemented by the concrete class.")

    def set_executor_interface(self, new_executor_interface):
        self.executor_interface = new_executor_interface

    def get_id(self):
        return self.id

    def get_task_output(self):
        raise Exception("ERROR: The get_task_output method must be implemented by the concrete class.")

    def setup(self, executor_interface, execution_group_info):
        self.executor_interface = executor_interface
        self.execution_group_info = execution_group_info
