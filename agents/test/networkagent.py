import os
import json

import agents.test.network as network
from agents.NetworkAgent import NetworkAgent
from agents import MESSAGE_SET_UPLINK

from message import Broker
from message import SEND_TO_CHILD, SEND_TO_PARENT
from message.Message import Message
from message.Agent import Agent

log = []

def show_logs():
    for nid, msg in log:
        print(nid, msg.json())

    for node in nodes:
        print(node.node.node_id, "meta table")
        for i in node.broker.meta_table().db.items():
            print('\t'+str(i))

class LogAgent(Agent):
    path = '/'
    def __init__(self, node_id: str, broker: Broker):
        super().__init__(broker)
        self.node_id = node_id

    def handler(self, msg: Message):
        log.append((self.node_id, msg))

class Node:
    def __init__(self, node: network.Node):
        self.node = node

        path = '/tmp/upy/mesh/'+node.node_id+'/data'

        try:
            os.stat(path)
        except:
            os.makedirs(path)
        else:
            print("DELETE DIRS")
            os.system('rm -rf '+path)
        
        self.broker = broker = Broker(path, node.node_id, 30)
        self.log_agent = LogAgent(node.node_id, broker)

        self.net_agent = NetworkAgent(broker)

nodes = [Node(node) for node in network.NODES]
nodeMap = {node.node.node_id:node for node in nodes}

def exchange_messages() -> bool:
    exchanged = False
    for node in nodes:
        if not node.net_agent.parent or node.net_agent.parent not in nodeMap:
            continue
        parent_node = nodeMap[node.net_agent.parent]
        print("--> Sync %s with %s" % (node.node.node_id, node.net_agent.parent))

        for key, raw_msg in node.broker.messages_to_send(dir=SEND_TO_PARENT):
            if key not in parent_node.broker.meta_table().db:
                exchanged = True
                print("exchanged", key)
                parent_node.broker.record_raw(raw_msg)
                node.broker.message_was_sent(key)
            # else:
            #     print("msg already shared with parent:", key)

        for key, raw_msg in parent_node.broker.messages_to_send(dir=SEND_TO_CHILD):
            if key not in node.broker.meta_table().db:
                exchanged = True
                print("exchanged", key)
                node.broker.record_raw(raw_msg)
                parent_node.broker.message_was_sent(key)
            # else:
            #     print("msg already shared to childred:", key)

    return exchanged

show_logs()

lastParentMap = {}
for i in range(2*len(nodes)):
    exchange_messages()
    #show_logs()
    parentMap = {node.node.node_id:node.net_agent.parent for node in nodes}
    print("PARENT MAP:", parentMap)
    if parentMap == lastParentMap and any(v == '' for v in parentMap.values()):
        print("SUCCESS after %d rounds!" % i)
        break
    lastParentMap = parentMap
else:
    assert False, "FAILED :("

#show_logs()

nodes[0].net_agent.write_local('/system/network', json.dumps({
    't': MESSAGE_SET_UPLINK,
    'e': 'UPINK-STATION',
    'p': 'test123',
}))

# lastParentMap = {}
i = 0
while exchange_messages():
    #show_logs()
    parentMap = {node.node.node_id:node.net_agent.parent for node in nodes}
    print("PARENT MAP:", parentMap)
    i += 1

    if all(node.net_agent.hop_count < len(nodes) for node in nodes):
        break
#     if parentMap == lastParentMap and any(v == '' for v in parentMap.values()):
#         print("SUCCESS after %d rounds!" % i)
#         break
#     lastParentMap = parentMap
# else:
#     assert False, "FAILED :("
print("DONE after %d rounds" % i)
for node in nodes:
    print("hop count:", node.node.node_id, node.net_agent.hop_count)
#show_logs()