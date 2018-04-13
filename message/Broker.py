import time

from store.Key import Key
from store.Store import Store
from store.Table import Table
from message.Message import Message
from message import (ACTION_RECEIVED, ACTION_WRITE, ACTION_REQUEST,
    ACTION_RESPONSE, DEST_LOCAL, DEST_UPLINK, DEST_NODE)
from message.Agent import Agent

class Broker(Store):

    def __init__(self, store_path: str, node_id: bytes, expiry: int, is_uplink=False):
        super().__init__(store_path)
        self.node_id = node_id
        self.expiry = expiry
        self.is_uplink = is_uplink
        self.subscriptions = {}
    
    def meta_table(self) -> Table:
        return self.open_table('/meta', mode='w')

    def record(self, msg: Message):
        if msg.action == ACTION_RECEIVED:
            key = msg.key.string()
            meta = self.meta_table()
            if key in meta.db:
                del meta.db[key]

        elif msg.action == ACTION_WRITE:
            self.write(msg.path, msg.key.string(), msg.value)
            if msg.dest != DEST_LOCAL:
                if msg.dest == DEST_NODE and msg.dest_node == self.node_id:
                    return
                self._record_meta(msg)
        
        elif msg.action == ACTION_REQUEST:
            try:
                # If we have the requested data, create a response message with that data
                key, val = self.latest(msg.path)
                msg.key = Key(key)
                msg.value = val
                msg.action = ACTION_RESPONSE
                self._record_meta(msg)
            except ValueError:
                # Otherwise just store the message to forward it to other nodes
                self._record_meta(msg)

        elif msg.action == ACTION_RESPONSE:
            if (msg.dest == DEST_NODE and msg.dest_node == self.node_id) or \
                (msg.dest == DEST_UPLINK and self.is_uplink):
                self.write(msg.path, msg.key.string(), msg.value)

        self.notify_agents(msg)

    def _record_meta(self, msg: Message):
        key = msg.key.string()
        self.write('/meta', key, msg.json())

    def notify_agents(self, msg: Message):
        for path, handlers in self.subscriptions.items():
            if msg.path.startswith(path):
                for handler in handlers:
                    handler(msg)

    def register(self, path: str, handler):
        handlers = self.subscriptions.get(path, [])
        handlers.append(handler)
        self.subscriptions[path] = handlers

    # def prune_messages(self):
    #     """ prune messages after the expiry """
    #     now = time.time()
    #     for k in self.meta.db.keys():
    #         key = Key(k)
    #         if key.time < (now - self.expiry):
    #             del self.meta.db[k]

    #     self.meta.db.flush()