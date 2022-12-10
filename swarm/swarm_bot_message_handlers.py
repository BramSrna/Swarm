from swarm.message_types import MessageTypes

def handle_new_task_message(swarm_bot, message):
    message_payload = message.get_message_payload()

    swarm_bot.task_queue.append({"HOLDER_ID": message_payload["TASK_HOLDER"], "TASK_ID": message_payload["TASK_ID"]})

    swarm_bot.task_queue_has_values.set()

def handle_request_task_transfer_message(swarm_bot, message):
    message_payload = message.get_message_payload()
    msg_id = message.get_id()

    task = None
    task_id = message_payload["TASK_ID"]
    for i in range(len(swarm_bot.task_queue)):
        curr_task = swarm_bot.task_queue[i]
        if ("TASK" in curr_task) and (curr_task["TASK"].get_id() == task_id):
            task = curr_task["TASK"]
            break
        i += 1
    if task is not None:
        swarm_bot.task_queue.pop(i)
        if len(swarm_bot.task_queue) == 0:
            swarm_bot.task_queue_has_values.clear()
    swarm_bot.create_directed_message(message.get_sender_id(), MessageTypes.TASK_TRANSFER, {"TASK_ID": task_id, "TASK": task})

def handle_task_transfer_message(swarm_bot, message):
    swarm_bot.task_queue.append({"TASK": message.get_message_payload()["TASK"]})

    swarm_bot.task_queue_has_values.set()