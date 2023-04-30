class UpdateBlock(object):
    def __init__(self, expected_state, time_issued, new_value):
        self.expected_state = expected_state
        self.time_issued = time_issued
        self.new_value = new_value

    def get_path(self):
        return self.path

    def get_time_issued(self):
        return self.time_issued

    def get_new_value(self):
        return self.new_value

    def get_expected_state(self):
        return self.expected_state

    def __str__(self):
        return "Time issued: {}, state: {}, new value: {}".format(self.time_issued, self.expected_state, self.new_value)
