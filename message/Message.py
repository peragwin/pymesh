import time
import json
try:
    import uhashlib as hashlib
except ImportError:
    import hashlib
try:
    from ubinascii import hexlify
except ImportError:
    from binascii import hexlify

from store.Key import Key
from message import DEST_LOCAL

class Message:
    """ Message is a key-value pair that can be marshaled and passed to other nodes
        or put into storage. """

    def __init__(self, path: str, key: bytes, value, action: int, dest: int, dest_node: str = ''):
        self.path = path
        self.key = key
        self.value = value
        self.action = action
        self.dest = dest
        self.dest_node = dest_node

    def json(self) -> bytes:
        return bytes(json.dumps({
            'p': self.path,
            'k': str(self.key, 'utf-8'),
            'v': self.value,
            'a': self.action,
            'd': self.dest,
            'n': str(self.dest_node, 'utf-8'),
        }), 'utf-8')

    def to_dict(self) -> dict:
        return {
            'p': self.path,
            'k': str(self.key, 'utf-8'),
            'v': self.value,
            'a': self.action,
            'd': self.dest,
            'n': self.dest_node,
        }

def unmarshall_JSON(b: bytes) -> Message:
    d = json.loads(str(b, 'utf-8'))
    key = bytes(d['k'], 'utf-8')
    value = d['v']
    dest_node = d['n']
    return Message(d['p'], key, value, d['a'], d['d'], dest_node)

def message_from_dict(d: dict) -> Message:
    return Message(d['p'], d['k'], d['v'], d['a'], d['d'], d['n'])

def new_message(path: str, device_id: bytes, value: bytes, action: int, dest=DEST_LOCAL):
    tim = time.time()
    key = Key(tim=tim, device_id=device_id)
    
    sha = hashlib.sha256()
    sha.update(path)
    sha.update(str(tim))
    sha.update(device_id)
    sha.update(str(value))
    key.data_id = hexlify(sha.digest())[:8]

    return Message(path, key.string(), value, action, dest)
