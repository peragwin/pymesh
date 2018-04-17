import time

from store.Key import Key
from store.Store import Store
from store.Table import Table
from message.Message import Message, message_from_dict, unmarshall_JSON
from message import (ACTION_RECEIVED, ACTION_WRITE, ACTION_REQUEST,
    ACTION_RESPONSE, DEST_LOCAL, DEST_UPLINK, DEST_NODE, DEST_PARENT,
    SEND_TO_PARENT,SEND_TO_CHILD, DEST_CHILD, DEST_NEIGHBORS)
from message.Agent import Agent
from message.EventHandler import EventHandler, EVENT_AFTER_HANDLER, EVENT_BEFORE_HANDLER


class Broker(Store):

    def __init__(self, store_path: str, node_id: str, expiry: int, is_uplink=False):
        super().__init__(store_path)
        self.node_id = node_id
        self.expiry = expiry
        self.is_uplink = is_uplink
        self.subscriptions = {}
        self.event_handlers = {}
    
    def meta_table(self) -> Table:
        return self.open_table('/meta', mode='w')

    def record(self, msg: Message, force_record_meta=False, notify_agents=True):
        record_meta = False

        if msg.action == ACTION_RECEIVED:
            key = msg.key
            meta = self.meta_table()
            if key in meta.db:
                del meta.db[key]

        elif msg.action == ACTION_WRITE:
            self.write(msg.path, msg.key, msg.value)
            if msg.dest == DEST_LOCAL:
                record_meta = False
            elif msg.dest == DEST_NODE:
                record_meta = not (msg.dest_node == self.node_id)
            elif msg.dest == DEST_PARENT:
                record_meta = False
            elif msg.dest == DEST_CHILD:
                record_meta = False
            elif msg.dest == DEST_NEIGHBORS:
                record_meta = False
            else:
                record_meta = True
        
        elif msg.action == ACTION_REQUEST:
            record_meta = True
            try:
                # If we have the requested data, create a response message with that data
                key, val = self.latest(msg.path)
                msg.key = key
                msg.value = val
                msg.action = ACTION_RESPONSE
            except ValueError:
                # Otherwise just store the message to forward it to other nodes
                pass

        elif msg.action == ACTION_RESPONSE:
            if (msg.dest == DEST_NODE and msg.dest_node == self.node_id) or \
                (msg.dest == DEST_UPLINK and self.is_uplink):
                self.write(msg.path, msg.key, msg.value)

        if record_meta or force_record_meta:
            self._record_meta(msg)

        if notify_agents:
            self.notify_agents(msg)

    def _record_meta(self, msg: Message):
        self.write('/meta', msg.key, msg.to_dict())

    def record_raw(self, msg: bytes):
        self.record(unmarshall_JSON(str(msg, 'utf-8')))

    def notify_agents(self, msg: Message):
        for path, handlers in self.subscriptions.items():
            if msg.path.startswith(path):
                event_handlers = self.event_handlers.get(msg.path, [])
                # if event_handlers:
                #     assert False, event_handlers
                for ev in event_handlers:
                    if ev.when == EVENT_BEFORE_HANDLER:
                        self.record(ev.msg, ev.force_record_meta, ev.notify_agents)

                for handler in handlers:
                    handler(msg)

                for ev in event_handlers:
                    if ev.when == EVENT_AFTER_HANDLER:
                        print("@@@ exec event for", path, msg.path)
                        self.record(ev.msg, ev.force_record_meta, ev.notify_agents)

                if event_handlers:
                    del self.event_handlers[msg.path]

    def register(self, path: str, handler):
        handlers = self.subscriptions.get(path, [])
        handlers.append(handler)
        self.subscriptions[path] = handlers

    def messages_to_send(self, dir=SEND_TO_PARENT):
        for key, raw_msg in self.meta_table().db.items():
            msg = unmarshall_JSON(raw_msg)
            if ((msg.dest == DEST_PARENT) or (msg.dest == DEST_UPLINK)) and dir == SEND_TO_CHILD:
                continue
            if msg.dest == DEST_CHILD and dir == SEND_TO_PARENT:
                continue
            yield (key, msg.json())

    def message_was_sent(self, key: bytes):
        msg = unmarshall_JSON(self.meta_table().db[key])
        if msg.dest == DEST_PARENT or msg.dest == DEST_CHILD:
            del self.meta_table().db[key]

    def schedule_after(self, event_path: str, msg: Message, force_record_meta=False, notify_agents=True):
        ev = EventHandler(msg, EVENT_AFTER_HANDLER, force_record_meta, notify_agents)
        self._schedule(event_path, ev)

    def _schedule(self, event_path: str, event: EventHandler):
        handlers = self.event_handlers.get(event_path, [])
        handlers.append(event)
        self.event_handlers[event_path] = handlers
        print("Scheduled:", event_path, [(eh.msg.path, eh.msg.value) for eh in handlers])

    def schedule_before(self, event_path: str, msg: Message, force_record_meta=False, notify_agents=True):
        ev = EventHandler(msg, EVENT_BEFORE_HANDLER, force_record_meta, notify_agents)
        self._schedule(event_path, ev)

    # def prune_messages(self):
    #     """ prune messages after the expiry """
    #     now = time.time()
    #     for k in self.meta.db.keys():
    #         key = Key(k)
    #         if key.time < (now - self.expiry):
    #             del self.meta.db[k]

    #     self.meta.db.flush()