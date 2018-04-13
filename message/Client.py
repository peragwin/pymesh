from message import Broker
from message.Message import new_message
from message import ACTION_WRITE, DEST_LOCAL, DEST_BROADCAST

class Client:
    """ A Client can read and write to a Broker """
    def __init__(self, broker: Broker):
        self.broker = broker
        self.node_id = broker.node_id
    
    def read(self, path: str) -> bytes:
        try:
            _, value = self.broker.latest(path)
            return value
        except ValueError:
            return b''

    def read_str(self, path: str) -> str:
        return str(self.read(path), 'utf-8')

    def write_local(self, path: str, value: bytes):
        if isinstance(value, str):
            value = bytes(value, 'utf-8')
        msg = new_message(path, self.node_id, value, ACTION_WRITE, DEST_LOCAL)
        self.broker.record(msg)

    def write_broadcast(self, path: str, value: bytes):
        if isinstance(value, str):
            value = bytes(value, 'utf-8')
        msg = new_message(path, self.node_id, value, ACTION_WRITE, DEST_BROADCAST)
        self.broker.record(msg)
