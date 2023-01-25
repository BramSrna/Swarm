import threading
import time
import os
import yaml
import logging

from random import randint
from network_manager.network_node.message_channel.local_message_channel import LocalMessageChannel

from network_manager.network_node.message_channel.message_channel_user import MessageChannelUser

from network_manager.network_node.message_wrapper.local_message_wrapper import LocalMessageWrapper
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper
from network_manager.network_node.propagation_strategy.naive_propagation import NaivePropagation
from network_manager.network_node.propagation_strategy.smart_propagation import SmartPropagation
from network_manager.network_node.network_node_idle_listener_interface import NetworkNodeIdleListenerInterface


"""
NetworkNode

Class to represent network nodes. Can send and receive messages
between nodes or propagate messages accross the entire network.
"""


class NetworkNode(MessageChannelUser):
    def __init__(self, additional_config_path: str = None, additional_config_dict: dict = None):
        """
        init

        Create a new NetworkNode object

        @param additional_config_path [String] Path to a YAML file containing extra config information to use
        @param additional_config_dict [dict] Dictionary containing extra config information to use

        @return [NetworkNode] The new network node
        """
        self.id = id(self)

        self.logger = logging.getLogger('NetworkNode')

        self.msg_channels = {}

        self.sent_messages = {}
        self.rcvd_messages = {}

        self.msg_inbox = []
        self.msg_outbox = []

        self.run_node = threading.Event()
        self.msg_inbox_has_values = threading.Event()
        self.msg_outbox_has_values = threading.Event()

        self.run_node.set()
        self.msg_inbox_has_values.set()
        self.msg_outbox_has_values.set()

        self.num_ignored_msgs = 0

        self.idle_listeners = []

        self.num_processes = 0

        self.msg_handler_dict = {}
        for msg_type, handler in self.msg_handler_dict.items():
            self.assign_msg_handler(msg_type, handler)

        self.config = yaml.load(open(os.path.join(os.path.dirname(__file__), "./default_node_config.yml")), Loader=yaml.FullLoader)
        additional_config = {}
        if additional_config_path is not None:
            additional_config = yaml.load(open(os.path.join(os.path.dirname(__file__), additional_config_path)), Loader=yaml.FullLoader)

        for key, value in additional_config.items():
            self.config[key] = value

        if additional_config_dict is not None:
            for key, value in additional_config_dict.items():
                self.config[key] = value

        propagation_strategies = {
            "NaivePropagation": NaivePropagation,
            "SmartPropagation": SmartPropagation
        }

        message_channels = {
            "LocalMessageChannel": LocalMessageChannel
        }

        message_wrappers = {
            "LocalMessageWrapper": LocalMessageWrapper
        }

        self.propagation_strategy = propagation_strategies[self.config["propagation_strategy"]](self)
        self.message_channel_type = message_channels[self.config["message_channel"]]
        self.message_wrapper_type = message_wrappers[self.config["message_wrapper"]]

    def startup(self) -> None:
        """
        startup

        Starts the threads needed by the node to perform its core function.
        The threads created are:
            - msg_sender_loop: Runs the loop for sending messages
            - msg_receiver_loop: Runs the loop for receiving messages

        @param None

        @return None
        """
        if not self.run_node.is_set():
            raise Exception("ERROR: Network node is already running. Must call teardown before calling setup.")

        self.run_node.clear()
        self.msg_inbox_has_values.clear()
        self.msg_outbox_has_values.clear()

        thread = threading.Thread(target=self._msg_sender_loop)
        thread.start()
        thread = threading.Thread(target=self._msg_receiver_loop)
        thread.start()

    def teardown(self) -> None:
        """
        teardown

        Teardown the network node. Stops all currently running threads
        in the object.

        @param None

        @return None
        """
        self.run_node.set()
        self.msg_inbox_has_values.set()
        self.msg_outbox_has_values.set()

    def assign_msg_handler(self, msg_type: str, handler: object):
        """
        assign_msg_handler

        Assign the given handler method to the given message type.
        The handler method will be called whenever a message of
        that type is received.

        @param msg_type [String] The message type associated with the handler
        @param handler [Method] The method to call when the given message type is received

        @return None
        """
        self.msg_handler_dict[msg_type] = handler

    def get_id(self) -> int:
        """
        get_id

        Returns the ID of the NetworkNode

        @param None

        @return [int] The ID of the NetworkNode
        """
        return self.id

    def connect_to_network_node(self, new_network_node: "NetworkNode") -> None:
        """
        connect_to_network_node

        Connects this NetworkNode to the given NetworkNode.
        Note that this is a one way connection. To make the
        connection bidirectional, you must call this method
        with the nodes swapped as well.

        @param new_network_node [NetworkNode] The NetworkNode to connect to

        @return None
        """
        if not isinstance(new_network_node, NetworkNode):
            raise Exception("ERROR: Can only connect to other NetworkNode objects.")

        node_id = new_network_node.get_id()
        if node_id not in self.msg_channels:
            self.msg_channels[node_id] = self.message_channel_type(self, new_network_node)

    def disconnect_from_network_node(self, id_to_disconnect: int) -> None:
        """
        disconnect_from_network_node

        Disconnects this network node from the given network node.
        Note that this only removes the connection in on direction.
        If the nodes are connecte din nodeh directions, then this
        method must be called with the nodes swapped to fully
        remove the connection. This method also does not protect
        against orphaned nodes, so make sure to check for that
        before calling this method.

        @param id_to_disconnect [int] The ID of the node to disconnect from

        @return None
        """
        if id_to_disconnect in self.msg_channels.keys():
            self.msg_channels.pop(id_to_disconnect)

    def is_connected_to(self, network_node_id: int) -> bool:
        """
        is_connected_to

        Returns the connection status between this node and
        the node with the given ID

        @param network_node_id [int] The ID of the node to check

        @return [bool] True if the two nodes are connected. False otherwise.
        """
        return network_node_id in self.msg_channels

    def get_connections(self) -> list:
        """
        get_connections

        Returns a list containing the IDs of the nodes that this
        node is connected to.

        @param None

        @return [list] The IDs of the nodes this one is connected to
        """
        return list(self.msg_channels.keys())

    def receive_message(self, message: MessageWrapper) -> None:
        """
        receive_message

        Adds the given message to this node's message inbox

        @param message [MessageWrapper] The message to add to the inbox

        @return None
        """
        self.msg_inbox.append(message)
        self.msg_inbox_has_values.set()

    def send_propagation_message(self, message_type: str, message_payload: dict) -> int:
        """
        send_propagation_message

        Creates a message to propagate accross the network and adds it to the
        message outbox.

        @param message_type [str] The type of message to create
        @param message_payload [dict] The payload for the message
        @param message_id [int] The ID to use for the message. If None is given, then a new ID is generated.
        @param msg_ref [MessageWrapper] The message that this message is being created in response to receiving.

        @return [int] The ID of the new message
        """
        targets = self.propagation_strategy.determine_prop_targets(None)

        message_id = self._generate_message_id()

        for target_node_id in targets:
            self._create_message(target_node_id, message_id, message_type, message_payload, True)

        return message_id

    def send_directed_message(self, target_node_id: int, message_type: str, message_payload: dict) -> int:
        """
        send_directed_message

        Creates a message to send directly to a given node and adds it to the
        message outbox.

        @param target_node_id [int] The ID of the node to send the message to
        @param message_type [str] The type of message to create
        @param message_payload [dict] The payload for the message

        @return [int] The ID of the new message
        """
        message_id = self._generate_message_id()

        return self._create_message(target_node_id, message_id, message_type, message_payload, False)

    def get_message_channels(self) -> None:
        """
        get_message_channels

        Returns a dictionary containing the node's current message channels

        @param None

        @return [dict<int, MessageChannel>] Dictionary containing the node's message channels
        """
        return self.msg_channels

    def received_msg_with_id(self, msg_id: int) -> bool:
        """
        received_msg_with_id

        Checks whether or not the node has received a message with the given ID

        @param msg_id [int] The message ID to check for

        @return [bool] True if the node has received a message with the given ID. False otherwise.
        """
        return msg_id in self.rcvd_messages

    def sent_msg_with_id(self, msg_id: int) -> bool:
        """
        sent_msg_with_id

        Checks whether or not the node has sent a message with the given ID

        @param msg_id [int] The message ID to check for

        @return [bool] True if the node has sent a message with the given ID. False otherwise.
        """
        return msg_id in self.sent_messages

    def interacted_with_msg_with_id(self, msg_id: int) -> bool:
        """
        interacted_with_msg_with_id

        Checks whether or not the node has interacted with a message with the
        given ID. Interacted means the node has either received or sent a
        message with the ID.

        @param msg_id [int] The message ID to check for

        @return [bool] True if the node has interacted with a message with the given ID. False otherwise.
        """
        return ((msg_id in self.rcvd_messages) or (msg_id in self.sent_messages))

    def get_sent_messages(self) -> dict:
        """
        get_sent_messages

        Returns a dictionary containing information about the messages
        the node has sent. The keys of the dictionary are the IDs of
        the messages the node has sent. The values of the dictionary
        are tuples where the first value is the type of message and
        the second value is the number of times that message was sent.

        @param None

        @return [dict<int, tuple>] Information about the messages sent by the node
        """
        sent_msgs = {}
        for _, msg_info in self.sent_messages.items():
            msg_id = msg_info["SENT_MSG"].get_id()
            msg_type = msg_info["SENT_MSG"].get_message_type()
            sent_msgs[msg_id] = (msg_type, msg_info["NUM_TIMES_SENT"])
        return sent_msgs

    def get_received_messages(self) -> dict:
        """
        get_received_messages

        Returns a dictionary containing information about the messages
        the node has received. The keys of the dictionary are the IDs of
        the messages the node has received. The values of the dictionary
        are tuples where the first value is the type of message and
        the second value is the number of times that message was received.

        @param None

        @return [dict<int, tuple>] Information about the messages received by the node
        """
        rcvd_msgs = {}
        for _, msg_info in self.rcvd_messages.items():
            msg_id = msg_info["MSG"].get_id()
            msg_type = msg_info["MSG"].get_message_type()
            rcvd_msgs[msg_id] = (msg_type, msg_info["NUM_TIMES_RCVD"])
        return rcvd_msgs

    def get_num_ignored_msgs(self) -> int:
        """
        get_num_ignored_msgs

        Returns the number of messages that the node has received and ignored

        @param None

        @return [int] The number of ignored messages
        """
        return self.num_ignored_msgs

    def add_idle_listener(self, new_listener: NetworkNodeIdleListenerInterface) -> None:
        """
        add_idle_listener

        Adds the given object as a new listener for the node

        @param new_listener [NetworkNodeIdleListenerInterface] The new listener for the node

        @return None
        """
        if not isinstance(new_listener, NetworkNodeIdleListenerInterface):
            raise Exception("ERROR: The listeners for the NetworkNode must implement the NetworkNodeIdleListenerInterface class.")
        self.idle_listeners.append(new_listener)

    def is_idle(self) -> bool:
        """
        is_idle

        Returns the idle status of the node

        @param None

        @return [bool] True if the node is idle. False otherwise.
        """
        return self.num_processes == 0

    def _msg_sender_loop(self) -> None:
        """
        msg_sender_loop

        Handles sending messages in the node's outbox. This method
        will run so long as the run_node flag is not set. The loop
        first checks if a message is present in the outbox. If no
        message is present, then the loop waits for one to be available.
        If a message is present, then the loop pops the first messsage
        from the outbox and sends it according to the messages configuration.

        @param None

        @return None
        """
        while (not self.run_node.is_set()):
            self.msg_outbox_has_values.wait()
            while (len(self.msg_outbox) > 0) and (not self.run_node.is_set()):
                self._notify_process_state(True)
                msg_to_send = self.msg_outbox.pop(0)

                message = msg_to_send["MESSAGE"]

                targets = [msg_to_send["TARGET_ID"]]

                for target_node_id in targets:
                    if target_node_id not in self.msg_channels:
                        raise Exception("ERROR: Tried to send message to unknown node ID: {}. Known node list: {}".format(str(target_node_id), self.msg_channels.keys()))

                if len(targets) == 0:
                    raise Exception("ERROR: Tried to send message with no targets: {}".format(str(message)))

                msg_id = message.get_id()

                if msg_id not in self.sent_messages:
                    self.sent_messages[msg_id] = {"TIME_SENT": time.time(), "SENT_MSG": message, "NUM_TIMES_SENT": 0}

                self.sent_messages[msg_id]["NUM_TIMES_SENT"] += 1

                for node_id in targets:
                    self.logger.debug("Sent message. Sender node ID: {}, target node ID: {}, message ID {}, message type: {}, sender message list {}\n\n".format(self.get_id(), target_node_id, msg_id, message.get_message_type(), self.sent_messages))
                    self.msg_channels[node_id].send_message(message)

                self._notify_process_state(False)

            if len(self.msg_inbox) == 0:
                self.msg_outbox_has_values.clear()

    def _msg_receiver_loop(self) -> None:
        """
        msg_receiver_loop

        Handles receiving messages in the node's inbox. This method
        will run so long as the run_node flag is not set. The loop
        first checks if a message is present in the inbox. If no
        message is present, then the loop waits for one to be available.
        If a message is present, then the loop pops the first messsage
        from the inbox and calls the handler associated with the message
        type. If the message is configured to be propagated, then the
        loop will propagate the message.

        @param None

        @return None
        """
        while not self.run_node.is_set():
            self.msg_inbox_has_values.wait()
            while (len(self.msg_inbox) > 0) and (not self.run_node.is_set()):
                self._notify_process_state(True)

                message = self.msg_inbox.pop(0)

                target_id = message.get_target_node_id()
                msg_id = message.get_id()
                message_type = str(message.get_message_type())
                message_payload = message.get_message_payload()
                should_propagate = message.get_propagation_flag()

                self.logger.debug("Received message. receiver node ID: {}, target node ID: {}, message ID {}, message type {}, payload: {}\n\n".format(self.get_id(), target_id, msg_id, message_type, message_payload))

                sender_id = message.get_sender_id()
                if (sender_id is not None) and (sender_id != self.get_id()) and (sender_id not in self.msg_channels):
                    raise Exception("ERROR: Received message from unknown node: {}. Known node list: {}".format(str(sender_id), self.msg_channels.keys()))

                if (self.interacted_with_msg_with_id(msg_id)):
                    if msg_id not in self.rcvd_messages:
                        self.rcvd_messages[msg_id] = {"MSG": message, "NUM_TIMES_RCVD": 0}
                    self.rcvd_messages[msg_id]["NUM_TIMES_RCVD"] += 1
                    self.num_ignored_msgs += 1
                    if should_propagate:
                        self.propagation_strategy.track_message_propagation(message)
                else:
                    self.rcvd_messages[msg_id] = {"MSG": message, "NUM_TIMES_RCVD": 1}

                    if message_type in self.msg_handler_dict:
                        self.msg_handler_dict[message_type](self, message)
                    else:
                        self.logger.warning("Warning: Received message type with no assigned handler: " + str(message_type))

                    if should_propagate:
                        targets = self.propagation_strategy.determine_prop_targets(message)

                        for target_node_id in targets:
                            self._create_message(target_node_id, msg_id, message_type, message_payload, True)

                self._notify_process_state(False)

            if len(self.msg_inbox) == 0:
                self.msg_inbox_has_values.clear()

    def _generate_message_id(self):
        """
        _generate_message_id

        Generates a new message ID. Ensures that the message ID
        generated has not been used before.

        @param None

        @return [int] The generated message ID
        """
        min_id = 0
        max_id = 1000000

        message_id = randint(min_id, max_id)
        while self.interacted_with_msg_with_id(message_id):
            message_id = randint(min_id, max_id)

        return message_id

    def _create_message(self, target_node_id, message_id, message_type, message_payload, propagate_message):
        """
        _create_message

        Creates a new message and adds it to the message outbox to be sent.

        @param target_node_id [int] The ID of the node to send the message to
        @param message_id [int] The ID of the message to send
        @param message_type [str] The type of the message to send
        @param message_payload [dict] The payload of hte message to send
        @param propagate_message [bool] Whether or not to propage the message

        @return [int] The message ID used for the message
        """
        if target_node_id not in self.msg_channels:
            raise Exception("ERROR: Tried to create message for unknown node ID: " + str(target_node_id))

        new_msg = self.message_wrapper_type(message_id, self.get_id(), target_node_id, message_type, message_payload, propagate_message)

        self.msg_outbox.append({"MESSAGE": new_msg, "TARGET_ID": target_node_id})
        self.msg_outbox_has_values.set()

        return message_id

    def _notify_process_state(self, process_running: bool) -> None:
        """
        notify_process_state

        Notifys the nodes idle listeners about the current statue of the node
        when a process starts/stops running

        @param notify_process_state [bool] True if a new process is running. False otherwise.

        @return None
        """
        self.logger.debug("Notify process state for node {}. State: {}".format(self.get_id(), process_running))

        curr_state = self.is_idle()

        if process_running:
            self.num_processes += 1
        else:
            self.num_processes -= 1

        new_state = self.is_idle()

        if curr_state != new_state:
            for listener in self.idle_listeners:
                listener.notify_idle_state(self.get_id(), new_state)
