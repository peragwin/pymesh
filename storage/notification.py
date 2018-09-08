
from storage.Key import Key

DEST_LOCAL = 1
DEST_BROADCAST = 2
DEST_UPLINK = 4
DEST_NODE = 8
DEST_PARENT = 16
DEST_CHILD = 32

ACTION_WRITE = 1
ACTION_RECEIVED = 2
ACTION_REQUEST = 3
ACTION_RESPONSE = 4
ACTION_SNAPSHOT = 5

class Meta:
    def __init__(self, destination: int, action: int):
        self.destination = destination
        self.action = action


class Notification:
    def __init__(self, key: Key, value: object, meta: Meta=None):
        self.key = key
        self.value = value
        self.meta = meta
