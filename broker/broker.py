
from storage.Store import Store
from storage.queue import Queue, LocalStorage
from storage.Key import Key

def deviceKey(device_id: str) -> Key:
    return Key(device_id, None, None, None)


class Broker:

    # path in local storage where a list of existing queues is stored
    QUEUE_IDS_PATH = "/ext/queues"

    def __init__(self, store: Store, device_id: str):
        self.store = store
        self.device_id = device_id

        self.localStorage = LocalStorage(store, device_id)
        
        # map of device_id -> Queue
        self.queues = {}
        self._initQueues()

    def _initQueues(self):
        """ Read a list of known queues from local storage and initialize
            the queues map """

        self.newQueue(self.device_id)

        ns = self.localStorage.getRangeLocal(self.QUEUE_IDS_PATH)
        for n in ns:
            self.newQueue(n.key)

    def newQueue(self, device_id: str):
        """ Create a new queue if one does not already exist for that device ID """

        if device_id in self.queues:
            return

        self.queues[device_id] = Queue(self.store, deviceKey(device_id))

        