import json

MESSAGE_HELLO = 'HELLO'
MESSAGE_SEND_HELLO = 'SEND_HELLO'
MESSAGE_SET_PARENT = 'SET_PARENT'
MESSAGE_UPLINK = 'UPLINK'
MESSAGE_SET_UPLINK = 'SET_UPLINK '

NETWORK_SEND_HELLO = b'{"t":"SEND_HELLO"}'
def network_set_parent(parent: str):
    return bytes(json.dumps({
        't': MESSAGE_SET_PARENT,
        'p': parent,
    }), 'utf-8')

from agents.NetworkAgent import NetworkAgent
