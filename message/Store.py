import bson

from store.Store import Store
from message.Message import Message

class MessageRecorder(Store):

    @staticmethod
    def new_record_state(msg: Message) -> tuple:
        path = msg.path + b':' + msg.key.string()
        value = bytes(bson.dumps({
            'l': True,
            'r': False,
        }), 'utf-8')
        return (path, value)

    def __init__(self, store_path: str):
        super().__init__(store_path)

        self.meta = self.open_table(store_path + '/meta')

    def record(self, msg: Message):
        self.write(msg.path, msg.key, msg.value)
        self._record_meta(msg)

    def _record_meta(self, msg: Message):
        path, value = self.new_record_state(msg)
        self.meta[path] = value