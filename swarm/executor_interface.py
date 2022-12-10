class ExecutorInterface(object):
    def __init__(self, swarm_bot):
        self.swarm_bot = swarm_bot

    def read_from_sensor(self, sensor_id, additional_params):
        return self.swarm_bot.read_from_sensor(sensor_id, additional_params)
