import time
import uhashlib
import bson
from store.util import Key

ACTION_WRITE = 1
ACTION_RECEIVED = 2
ACTION_REQUEST = 3
ACTION_SNAPSHOT = 4

class Message:
    """ Message is a key-value pair that can be marshaled and passed to other nodes
        or put into storage. """

    def __init__(self, path: str, key: Key, value: bytes, action: int):
        self.path = path
        self.key = key
        self.value = value
        self.action = action

    def marshall_BSON(self) -> bytes:
        return bytes(bson.dumps({
            'p': self.path,
            'k': self.key.string(),
            'v': self.value,
            'a': self.action,
        }), 'utf-8')

    def unmarshall_BSON(self, b: bytes):
        d = bson.loads(str(b))
        self.path = d['p']
        self.key = Key(d['k'])
        self.value = d['v']
        self.action = d['a']

def new_message(path: str, device_id: bytes, value: bytes, action: int):
    key = Key(tim=time.time(), device_id=device_id)
    
    sha = uhashlib.sha256()
    sha.update(path)
    sha.update(key.string())
    sha.update(value)
    key.data_id = sha.digest()[:8]

    return Message(path, key, value, action)



