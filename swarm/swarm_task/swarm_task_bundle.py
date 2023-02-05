class SwarmTaskBundle(object):
    def __init__(self):
        self.id = id(self)

        self.tasks = []
        self.task_ids = []

    def add_task(self, task_type, num_bots):
        for _ in range(num_bots):
            new_task = task_type()
            self.tasks.append(new_task)
            self.task_ids.append(new_task.get_id())

    def get_req_num_bots(self):
        return len(self.tasks)

    def get_tasks(self):
        return self.tasks

    def get_task_ids(self):
        return self.task_ids

    def is_complete(self):
        complete = True
        for task in self.tasks:
            complete = complete and task.is_complete()
        return complete

    def get_id(self):
        return self.id

    def status_to_str(self):
        status = "Status: \n"
        for task in self.tasks:
            status += "\t{}: {}\n".format(task.get_id(), task.is_complete())
        return status
