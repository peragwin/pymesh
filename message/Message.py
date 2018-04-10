import time
import uhashlib
from ubinascii import hexlify
import json
from store.Key import Key

ACTION_WRITE = 1
ACTION_RECEIVED = 2
ACTION_REQUEST = 3
ACTION_RESPONSE = 4
ACTION_SNAPSHOT = 5

DEST_LOCAL = 0
DEST_BROADCAST = 1
DEST_UPLINK = 2
DEST_NODE = 3

class Message:
    """ Message is a key-value pair that can be marshaled and passed to other nodes
        or put into storage. """

    def __init__(self, path: str, key: Key, value: bytes, action: int, dest: int, dest_node: bytes = b''):
        self.path = path
        self.key = key
        self.value = value
        self.action = action
        self.dest = dest
        self.dest_node = dest_node

    def marshall_JSON(self) -> bytes:
        return bytes(json.dumps({
            'p': self.path,
            'k': self.key.string(),
            'v': self.value,
            'a': self.action,
            'd': self.dest,
            'n': self.dest_node,
        }), 'utf-8')

def unmarshall_JSON(b: bytes) -> Message:
    d = json.loads(str(b, 'utf-8'))
    key = Key(bytes(d['k'], 'utf-8'))
    value = bytes(d['v'], 'utf-8')
    dest_node = bytes(d['n'], 'utf-8')
    return Message(d['p'], key, value, d['a'], d['d'], dest_node)

def new_message(path: str, device_id: bytes, value: bytes, action: int, dest=DEST_LOCAL):
    key = Key(tim=time.time(), device_id=device_id)
    
    sha = uhashlib.sha256()
    sha.update(path)
    sha.update(key.string())
    sha.update(value)
    key.data_id = hexlify(sha.digest())[:8]

    return Message(path, key, value, action, dest)



