from message import Broker
from message.Message import new_message
from message import ACTION_WRITE, DEST_LOCAL, DEST_BROADCAST, DEST_PARENT, DEST_NEIGHBORS

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

    def write_parent(self, path: str, value: bytes):
        """ Write message to the parent. Force record meta so that it gets sent to parent.
            Normally when we receive a message addressed to a parent, we wouldn't forward it. """
        if isinstance(value, str):
            value = bytes(value, 'utf-8')
        msg = new_message(path, self.node_id, value, ACTION_WRITE, DEST_PARENT)
        self.broker.record(msg, notify_agents=False, force_record_meta=True)

    def write_neighbors(self, path: str, value: bytes):
        """ Write message to parent and to child but don't forward it. """
        if isinstance(value, str):
            value = bytes(value, 'utf-8')
        msg = new_message(path, self.node_id, value, ACTION_WRITE, DEST_NEIGHBORS)
        print("@@@ record msg for neighbors", msg.json())
        self.broker.record(msg, notify_agents=False, force_record_meta=True)
 