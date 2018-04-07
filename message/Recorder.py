import time
import bson

from store.util import Key
from store.Store import Store
from message.Message import Message, ACTION_RECEIVED, ACTION_WRITE, ACTION_REQUEST

class MessageRecorder(Store):

    def __init__(self, store_path: str, expiry: int):
        super().__init__(store_path)
        self.expiry = expiry
        self.meta = self.open_table(store_path + '/meta')

    @staticmethod
    def new_record_state(msg: Message) -> tuple:
        path = msg.key.string() + b':' + msg.path
        return (path, False)

    @staticmethod
    def parse_record_state(s: bytes) -> Message:
        key_s, path = s.split(b':')
        key = Key(key_s)
        return Message(path, key, b'', 0)

    def record(self, msg: Message):
        if msg.action == ACTION_RECEIVED:
            path, _ = self.new_record_state(msg)
            if path in self.meta.db:
                del self.meta.db[path]
            return

        elif msg.action == ACTION_WRITE:
            self.write(msg.path, msg.key, msg.value)
            self._record_meta(msg)
            return
        
        elif msg.action == ACTION_REQUEST:
            return self.read(msg.path, msg.key)

    def _record_meta(self, msg: Message):
        path, value = self.new_record_state(msg)
        self.meta[path] = value

    def prune(self):
        """ prune messages after the expiry """
        for k in self.meta.db.keys():
            key = self.parse_record_state(k).key
            if key.time < (time.time() - self.expiry):
                del self.meta.db[k]