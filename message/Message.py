import time
import uhashlib
import json
from store.Key import Key

ACTION_WRITE = 1
ACTION_RECEIVED = 2
ACTION_REQUEST = 3
ACTION_SNAPSHOT = 4

DEST_LOCAL = b'0'
DEST_BROADCAST = b'1'
DEST_UPLINK = b'2'
DEST_NODE = b'3'

class Message:
    """ Message is a key-value pair that can be marshaled and passed to other nodes
        or put into storage. """

    def __init__(self, path: str, key: Key, value: bytes, action: int, dest: bytes):
        self.path = path
        self.key = key
        self.value = value
        self.action = action
        self.dest = dest

    def marshall_JSON(self) -> bytes:
        return bytes(json.dumps({
            'p': self.path,
            'k': self.key.string(),
            'v': self.value,
            'a': self.action,
            'd': self.dest
        }), 'utf-8')

    def unmarshall_JSON(self, b: bytes):
        d = json.loads(str(b))
        self.path = d['p']
        self.key = Key(d['k'])
        self.value = d['v']
        self.action = d['a']
        self.dest = d['d']

def new_message(path: str, device_id: bytes, value: bytes, action: int, dest=DEST_LOCAL):
    key = Key(tim=time.time(), device_id=device_id)
    
    sha = uhashlib.sha256()
    sha.update(path)
    sha.update(key.string())
    sha.update(value)
    key.data_id = sha.digest()[:8]

    return Message(path, key, value, action, dest)



