import time

from store.Key import Key
from store.Store import Store
from message.Message import (Message, ACTION_RECEIVED, ACTION_WRITE, ACTION_REQUEST,
    ACTION_RESPONSE, DEST_LOCAL, DEST_UPLINK, DEST_NODE)

class MessageRecorder(Store):

    def __init__(self, store_path: str, node_id: bytes, expiry: int, is_uplink=False):
        super().__init__(store_path)
        self.node_id = node_id
        self.expiry = expiry
        self.is_uplink = is_uplink
        self.meta = self.open_table('/meta', mode='w', cache_size=4096)

    def record(self, msg: Message):
        if msg.action == ACTION_RECEIVED:
            key = msg.key.string()
            if key in self.meta.db:
                del self.meta.db[key]

        elif msg.action == ACTION_WRITE:
            self.write(msg.path, msg.key.string(), msg.value)
            if msg.dest != DEST_LOCAL:
                if msg.dest == DEST_NODE and msg.dest_node == self.node_id:
                    return
                self._record_message(msg)
        
        elif msg.action == ACTION_REQUEST:
            try:
                # If we have the requested data, create a response message with that data
                key, val = self.latest(msg.path)
                msg.key = Key(key)
                msg.value = val
                msg.action = ACTION_RESPONSE
                self._record_message(msg)
            except ValueError:
                # Otherwise just store the message to forward it to other nodes
                self._record_message(msg)

        elif msg.action == ACTION_RESPONSE:
            if (msg.dest == DEST_NODE and msg.dest_node == self.node_id) or \
                (msg.dest == DEST_UPLINK and self.is_uplink):
                self.write(msg.path, msg.key.string(), msg.value)

    def _record_message(self, msg: Message):
        key = msg.key.string()
        self.meta.db[key] = msg.marshall_JSON()

    def prune_messages(self):
        """ prune messages after the expiry """
        now = time.time()
        for k in self.meta.db.keys():
            key = Key(k)
            if key.time < (now - self.expiry):
                del self.meta.db[k]

        self.meta.db.flush()