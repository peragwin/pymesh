
from typing import List
import time

from storage.Store import Store
from storage.queue import Queue, LocalStorage
from storage.Key import Key, Path
from storage.notification import Notification, DEST_BROADCAST, DEST_NODE

def deviceKey(device_id: str) -> Key:
    return Key(device_id, None, None, None)


class Router:
    """ Router manages a collection of Queues as well as local storage.
        Incomming messages are routed to the respective queue.
        Requests can be made for the offset of each queue, and for messages
        since some offset. """

    # path in local storage where a list of existing queues is stored
    QUEUE_IDS_PATH = Path("ext", "queues")

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
            self.newQueue(str(n.key.key, 'utf-8'))

    def newQueue(self, device_id: str) -> Queue:
        """ Create a new queue if one does not already exist for that device ID """

        if device_id in self.queues:
            return self.queues[device_id]

        q = Queue(self.store, deviceKey(device_id))
        self.queues[device_id] = q
        self.localStorage.storeLocal(self.QUEUE_IDS_PATH, bytes(device_id, 'utf-8'), None)
        return q

    def getOffset(self, device_id: str) -> int:
        """ Get the offset timestamp of the most recent message in a device's queue,
            or 0 if that queue does not exist. """
        if device_id in self.queues:
            return self.queues[device_id].getOffset()
        return 0

    def ingressHandler(self, ns: List[Notification]):
        """ ingressHandler routes incomming notifications to queues. """
        for n in ns:
            # handle incomming notifications destined for this node
            if n.meta.destination == DEST_NODE and n.meta.node == self.device_id:
                # check that the originating device is authorized to write to our storage 
                if self.isAuthorized(n.key):
                    self.localStorage.store([n])
                else:
                    # WHAT DO?
                    self.log("unauthorized request to local storage", n.key)  
                continue

            # otherwise save the notification to the appropriate queue so it can be
            # broadcast again
            dev = n.key.device_id            
            queue = self.newQueue(dev)
            queue.store([n])

    def isAuthorized(self, key: Key) -> bool:
        """ isAuthorized looks at a key and returns whether the originating device
            is authorized to write to that path in local storage """
        # TODO: have some authorization provider store permissions locally
        return True

    def log(self, *args):
        print("[{0}][{1}][LOG]:".format(self.device_id, time.time()), *args)
