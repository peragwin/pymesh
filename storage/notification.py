
from storage.Key import Key

DEST_BROADCAST = 1
DEST_NODE = 2

class Meta:
    def __init__(self, destination: int, node: str = ""):
        self.destination = destination
        self.node = node


class Notification:
    def __init__(self, key: Key, value: object, meta: Meta=None):
        self.key = key
        self.value = value
        self.meta = meta
