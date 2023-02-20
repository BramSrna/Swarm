class ExecutorInterface(object):
    def __init__(self, swarm_bot):
        self.swarm_bot = swarm_bot

    def get_id(self):
        return self.swarm_bot.get_id()

    def read_from_sensor(self, sensor_id, additional_params):
        return self.swarm_bot.read_from_sensor(sensor_id, additional_params)

    def send_propagation_message(self, message_type, message_payload):
        return self.swarm_bot.send_propagation_message(message_type, message_payload)

    def send_directed_message(self, target_node_id, message_type, message_payload, sync_message):
        return self.swarm_bot.send_directed_message(target_node_id, message_type, message_payload, sync_message)

    def respond_to_message(self, message, message_payload):
        return self.swarm_bot.respond_to_message(message, message_payload)

    def get_execution_group_ledger(self):
        return self.swarm_bot.get_execution_group_ledger()

    def add_new_execution_group_leader(self, task_bundle_id, owner_id):
        return self.swarm_bot.add_new_execution_group_leader(task_bundle_id, owner_id)

    def notify_task_completion(self, bundle_id):
        return self.swarm_bot.notify_task_completion(bundle_id)
