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

def sync_nodes(node_a: str, node_b: str, direction: int):
    from_node = nodeMap[node_a]
    to_node = nodeMap[node_b]
    for key, raw_msg in from_node.broker.messages_to_send(direction):
        if key not in to_node.broker.meta_table().db:
            to_node.broker.record_raw(raw_msg)
            from_node.broker.message_was_sent(key)

class SyncAgent(Agent):
    path = '/system/sync'

    def handler(self, msg: Message):
        if msg.path == '/system/sync/parent':
            parent = msg.value['p']
            if not parent:
                return
            sync_nodes(self.node_id, parent, SEND_TO_PARENT)

        elif msg.path == '/system/sync/child':
            child = msg.value['c']
            sync_nodes(self.node_id, child, SEND_TO_CHILD)

for node in nodes:
    node.sync_agent = SyncAgent(node.broker)

def exchange_messages() -> bool:
    exchanged = False
    for node in nodes:
        if not node.net_agent.parent or node.net_agent.parent not in nodeMap:
            node.net_agent.write_local('/system/sync/parent', {'p': None})
            continue
        parent_node = nodeMap[node.net_agent.parent].node.node_id
        print("--> Sync %s with %s" % (node.node.node_id, node.net_agent.parent))


        for key, _ in node.broker.messages_to_send(dir=SEND_TO_PARENT):
            if key not in nodeMap[parent_node].broker.meta_table().db:
                exchanged = True
                print("exchanged", key)
            # else:
            #     print("msg already shared with parent:", key)
        node.net_agent.write_local('/system/sync/parent', {'p': parent_node})



        for key, _ in nodeMap[parent_node].broker.messages_to_send(dir=SEND_TO_CHILD):
            if key not in node.broker.meta_table().db:
                exchanged = True
                print("exchanged", key)
            # else:
            #     print("msg already shared to child:", key)
        nodeMap[parent_node].net_agent.write_local('/system/sync/child', {'c': node.node.node_id})

    return exchanged

show_logs()

import micropython as mp

lastParentMap = {}
for i in range(2*len(nodes)):
    exchange_messages()
    #show_logs()
    parentMap = {node.node.node_id:node.net_agent.parent for node in nodes}
    print("PARENT MAP:", parentMap)
    mp.mem_info()
    if parentMap == lastParentMap and any(v == '' for v in parentMap.values()):
        print("SUCCESS after %d rounds!" % i)
        break
    lastParentMap = parentMap
else:
    assert False, "FAILED :("

#show_logs()
# assert False, 'DONE'

nodes[0].net_agent.write_local('/system/network', {
    't': MESSAGE_SET_UPLINK,
    'e': 'UPINK-STATION',
    'p': 'test123',
})

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

#show_logs()

print("DONE after %d rounds" % i)
for node in nodes:
    print("hop count:", node.node.node_id, node.net_agent.hop_count)
print(parentMap)

print("log size:", len(log))