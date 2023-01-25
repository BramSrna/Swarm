import threading

from swarm.message_types import MessageTypes


def handle_new_task_message(swarm_bot, message):
    message_payload = message.get_message_payload()

    swarm_bot.task_bundle_queue.append({"HOLDER_ID": message_payload["TASK_BUNDLE_HOLDER"], "TASK_BUNDLE_ID": message_payload["TASK_BUNDLE_ID"]})

    swarm_bot.task_bundle_queue_has_values.set()


def handle_request_task_bundle_transfer_message(swarm_bot, message):
    message_payload = message.get_message_payload()

    task = None
    task_bundle_id = message_payload["TASK_BUNDLE_ID"]
    for i in range(len(swarm_bot.task_bundle_queue)):
        curr_task = swarm_bot.task_bundle_queue[i]
        if ("TASK" in curr_task) and (curr_task["TASK"].get_id() == task_bundle_id):
            task = curr_task["TASK"]
            break
        i += 1
    if task is not None:
        swarm_bot.task_bundle_queue.pop(i)
        if len(swarm_bot.task_bundle_queue) == 0:
            swarm_bot.task_bundle_queue_has_values.clear()
    swarm_bot.send_directed_message(message.get_sender_id(), MessageTypes.TASK_BUNDLE_TRANSFER, {"TASK_BUNDLE_ID": task_bundle_id, "TASK": task})


def handle_task_bundle_transfer_message(swarm_bot, message):
    swarm_bot.task_bundle_queue.append({"TASK": message.get_message_payload()["TASK"]})

    swarm_bot.task_bundle_queue_has_values.set()


def handle_swarm_memory_object_location_message(swarm_bot, message):
    msg_payload = message.get_message_payload()
    object_id = msg_payload["OBJECT_ID"]
    location_id = msg_payload["LOCATION_ID"]
    swarm_bot.swarm_mem_loc_hash[object_id] = location_id


def handle_request_swarm_memory_read_message(swarm_bot, message):
    msg_payload = message.get_message_payload()
    object_key = msg_payload["KEY_TO_READ"]

    if object_key in swarm_bot.local_swarm_memory_contents:
        object_value = swarm_bot.local_swarm_memory_contents[object_key]

        swarm_bot.send_propagation_message(MessageTypes.TRANSFER_SWARM_MEMORY_VALUE, {"OBJECT_KEY": object_key, "OBJECT_VALUE": object_value})


def handle_transfer_swarm_memory_value_message(swarm_bot, message):
    msg_payload = message.get_message_payload()
    object_key = msg_payload["OBJECT_KEY"]
    object_value = msg_payload["OBJECT_VALUE"]

    if object_key not in swarm_bot.swarm_memory_cache:
        swarm_bot.swarm_memory_cache[object_key] = {
            "LOCK": threading.Condition(),
            "VALUE": None
        }
    swarm_bot.swarm_memory_cache[object_key]["VALUE"] = object_value
    with swarm_bot.swarm_memory_cache[object_key]["LOCK"]:
        swarm_bot.swarm_memory_cache[object_key]["LOCK"].notify_all()


def handle_delete_from_swarm_memory_message(swarm_bot, message):
    msg_payload = message.get_message_payload()
    key_to_delete = msg_payload["KEY_TO_DELETE"]

    if key_to_delete in swarm_bot.local_swarm_memory_contents:
        swarm_bot.local_swarm_memory_contents.pop(key_to_delete)
    if key_to_delete in swarm_bot.swarm_mem_loc_hash:
        swarm_bot.swarm_mem_loc_hash.pop(key_to_delete)


def handle_notify_task_bundle_execution_start_message(swarm_bot, message):
    message_payload = message.get_message_payload()

    task = None
    task_bundle_id = message_payload["TASK_BUNDLE_ID"]
    for i in range(len(swarm_bot.task_bundle_queue)):
        curr_task = swarm_bot.task_bundle_queue[i]
        if (("TASK" in curr_task) and (curr_task["TASK"].get_id() == task_bundle_id)) or (("TASK_BUNDLE_ID" in curr_task) and (curr_task["TASK_BUNDLE_ID"] == task_bundle_id)):
            task = curr_task
            break
        i += 1
    if task is not None:
        swarm_bot.task_bundle_queue.pop(i)
