from store.util import Key
import bson

class Message:
    """ Message is a key-value pair that can be marshaled and passed to other nodes
        or put into storage. """

    def __init__(self, path: str, key: Key, value: bytes):
        self.path = path
        self.key = key
        self.value = value

    def marshall_BSON(self) -> bytes:
        return bytes(bson.dumps({
            'p': self.path,
            'k': self.key,
            'v': self.value,
        }), 'utf-8')
