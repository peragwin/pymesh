import json

MESSAGE_HELLO = 'HELLO'
MESSAGE_SEND_HELLO = 'SEND_HELLO'
MESSAGE_SET_PARENT = 'SET_PARENT'
MESSAGE_UPLINK = 'UPLINK'
MESSAGE_SET_UPLINK = 'SET_UPLINK '

NETWORK_SEND_HELLO = {"t":"SEND_HELLO"}
def network_set_parent(parent: str):
    return {
        't': MESSAGE_SET_PARENT,
        'p': parent,
    }
    # return bytes(json.dumps({
    #     't': MESSAGE_SET_PARENT,
    #     'p': parent,
    # }), 'utf-8')

CONFIG_HOPS_PATH = '/config/network/hops'
SYSTEM_HOPS_PATH = '/system/network/hops'

CONFIG_FTP_PATH = '/config/ftp'
CONFIG_TELNET_PATH = '/config/telnet'

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


from agents.NetworkAgent import NetworkAgent

