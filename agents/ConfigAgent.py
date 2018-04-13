import json
from message.Subscriber import Subscriber
from message.Message import (Message, new_message, ACTION_WRITE, DEST_PARENT)

MESSAGE_HELLO = 'HELLO'

class ConfigAgent(Subscriber):
    """ ConfigAgent listens for messages at the /system path and is used
        to configure the network """
    def __init__(self):
        super().__init__('/system', self.message_handler)

    def start(self):
        self.send_hello(hops=[self.recorder.node_id])

    def send_hello(self, hops: list):
        node_id = self.recorder.node_id
        body = bytes(json.dumps({
            't': MESSAGE_HELLO,
            'h': hops,
        }), 'utf-8')
        self.recorder.record(
            new_message('/system', node_id, body,
                        ACTION_WRITE, DEST_PARENT))

    def message_handler(self, msg: Message):
        node_id = self.recorder.node_id
        if msg.action == ACTION_WRITE:
            body = json.loads(str(msg.value, 'utf-8'))
            msg_type = body['t']

            if msg_type == MESSAGE_HELLO:
                hops = body['h']

                if node_id in hops:
                    print("@@@ Cycle Detected, reconfiguring...")
                    self.recorder.record('/system/sta/reconfigure',
                        node_id, b'', ACTION_WRITE)

                else:
                    hops.append(node_id)
                    self.send_hello(hops)
