from message import Broker
from message.Client import Client

class Agent(Client):
    """ An Agent is a Client that watches a path for incoming messages and reacts
        via handler """

    path = None
    handler = None

    def __init__(self, broker: Broker):
        assert self.path is not None and self.handler is not None, \
            "path and handler must both be set by class implementing Agent"
        
        super().__init__(broker)
        broker.register(self.path, self.handler)

    