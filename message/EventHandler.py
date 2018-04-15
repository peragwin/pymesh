from message.Message import Message

EVENT_BEFORE_HANDLER = 0
EVENT_AFTER_HANDLER = 1

class EventHandler:
    def __init__(self, msg: Message, when: int, force_record_meta=False, notify_agents=True):
        self.msg = msg
        self.when = when
        self.force_record_meta = force_record_meta
        self.notify_agents = notify_agents