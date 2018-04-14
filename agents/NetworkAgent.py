import json

from message.Agent import Agent
from message.Message import Message, new_message
from message import ACTION_WRITE
from message import Broker

from agents.StationAgent import StationAgent
from agents.AccessPointAgent import AccessPointAgent

from agents import (MESSAGE_HELLO, MESSAGE_SEND_HELLO, MESSAGE_SET_PARENT,
    MESSAGE_UPLINK, MESSAGE_SET_UPLINK)

class NetworkAgent(Agent):
    """ ConfigAgent listens for messages at the /system path and is used
        to configure the network """
    path = '/system/network'

    def __init__(self, broker: Broker):
        super().__init__(broker)

        self.parent = None
        self.hop_count = 0x7FFFFFFF

        self.access_point_agent = AccessPointAgent(broker)
        self.station_agent = StationAgent(broker)

    def send_hello(self, hops: list):
        body = bytes(json.dumps({
            't': MESSAGE_HELLO,
            'h': hops,
        }), 'utf-8')
        self.write_parent('/system/network', body)

    def send_uplink(self, hops: list):
        body = bytes(json.dumps({
            't': MESSAGE_UPLINK,
            'h': hops,
        }), 'utf-8')
        self.write_neighbors('/system/network', body)

    def handler(self, msg: Message):
        node_id = self.node_id
        if msg.action == ACTION_WRITE:
            print("net agent handler:", msg.value)
            body = json.loads(str(msg.value, 'utf-8'))
            msg_type = body['t']

            if msg_type == MESSAGE_HELLO:
                hops = body['h']

                if self.parent in hops:
                    print("@@@ Cycle Detected on '%s'->'%s', reconfiguring..." % (node_id, self.parent))
                    self.write_local('/system/sta/reconfigure', json.dumps(hops))

                else:
                    hops.append(node_id)
                    self.send_hello(hops)

            elif msg_type == MESSAGE_SEND_HELLO:
                self.send_hello([self.node_id])

            elif msg_type == MESSAGE_SET_PARENT:
                self.parent = body['p']

            elif msg_type == MESSAGE_SET_UPLINK:
                self.hop_count = 0
                print('@@@ send uplink')
                self.send_uplink([self.node_id])

                # DO THIS AFTER ALL THE NODES HAVE RECEIVED THE UPLINK MESSAGES
                # self.write_local('/system/sta/connect', msg.value)
                # self.parent = body['e']

            elif msg_type == MESSAGE_UPLINK:
                print("@@@ receive uplink msg", self.node_id, body)
                hops = body['h']
                hop_count = len(hops)

                if hop_count < self.hop_count:
                    self.hop_count = hop_count
                    from_node = hops[-1]
                    hops.append(self.node_id)
                    self.send_uplink(hops)
                else:
                    print("@@@ ignored message")
                    

                    # DO THIS AFTER ALL THE NODES HAVE RECEIVED THE UPLINK MESSAGES
                    # self.write_local('/system/sta/connect', json.dumps({
                    #     'n': from_node,
                    # }))