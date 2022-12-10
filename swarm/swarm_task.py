class SwarmTask(object):
    def __init__(self):
        self.id = id(self)

        self.executor_interface = None

    def get_req_num_bots(self):
        return self.req_num_bots

    def execute_task(self):
        raise Exception("ERROR: The execute_task method must be implemented by the concrete class.")

    def is_task_complete(self):
        raise Exception("ERROR: The is_task_complete method must be implemented by the concrete class.")

    def set_executor_interface(self, new_executor_interface):
        self.executor_interface = new_executor_interface

    def get_id(self):
        return self.id