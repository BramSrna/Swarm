import time
import logging


"""
NetworkNodeIdleListenerInterface

Implements methods that can be used by listeners to
track the state of network nodes.
"""


class NetworkNodeIdleListenerInterface(object):
    def __init__(self):
        """
        init

        Creates a new NetworkNodeIdleListenerInterface object

        @param nil

        @return [NetworkNodeIdleListenerInterface] The newly created NetworkNodeIdleListenerInterface object
        """
        self.num_busy_nodes = 0
        self.logger = logging.getLogger('NetworkNode')

    def notify_idle_state(self, node_id: str, node_idle: bool) -> None:
        """
        notify_idle_state

        Called by the network nodes to notify listeners of
        a change in the idle state of the node.

        @param node_id [str] The ID of the node that changed state
        @param node_idle [bool] True if the node is now idle. False if the node is busy.

        @return None
        """
        if node_idle:
            self.num_busy_nodes -= 1
        else:
            self.num_busy_nodes += 1

    def network_is_idle(self) -> bool:
        """
        network_is_idle

        Returns the current state of the network.

        @param None

        @return [bool] True if the network is idle. False otherwise.
        """
        return self.num_busy_nodes == 0

    def wait_for_idle_network(self, timeout_sec: int = 10) -> bool:
        """
        wait_for_idle_network

        Waits for the network to be idle. The time to wait
        is specified by the timeout paramer value. If the timeout
        is hit before the network is idle, then an error is raised.

        @param timeout [Integer] The amount of seconds to wait for the network to become idle. Default is 10 seconds.

        @return [bool] True if the network becomes idle before the timeout is hit

        @raises [Exception] Raised if the network does not become idle before the timeout is hit.
        """
        start_time = time.time()
        while (time.time() < start_time + timeout_sec):
            if self.network_is_idle():
                time.sleep(3)
                if self.network_is_idle():
                    return True

        raise Exception("ERROR: Network was not idle before timeout was hit. # Busy Nodes: {}".format(self.num_busy_nodes))
