
try:
    from typing import List, Generator
except ImportError:
    pass

from storage import Base
from storage.Store import Store
from storage.Key import Key
from storage.notification import Notification

class Queue:
    """ A Queue is a single partition of the storage. Storage can be partitioned
        arbitrarily by any marker, but care should be taken to choose a partition
        marker that will not result in overlapping queues within the same storage.
        An offset is stored which marks the timestamp of the most recently written
        item.
        
        TODO: currenlty only the deviceID field of the key is used to generate a
        partition since the Store implementation creates both time and path indexed
        partitions.
    """

    def __init__(self, st: Store, key: Key):
        self._store = st
        if not self._store.hasPartition(key):
            self._store.createPartition(key)
        self._key = key

    def getOffset(self) -> int:
        try:
            return next(self._store.getRange(self._key, self._key, reverse=True,
                index=Base.BY_TIME_INDEX)).key.time
        except StopIteration:
            return 0

    def store(self, ns: List[Notification]):
        self._store.store(ns)

    def get(self, key: Key, index: chr = Base.BY_TIME_INDEX) -> Notification:
        return self._store.get(key, index)

    def getPath(self, path: str, key: bytes = None) -> Generator[Notification, None, None]:
        k = Key(self._key.device_id, None, path, key)
        return self._store.getRange(k, k, index=Base.BY_PATH_INDEX)

    def getFromOffset(self, offset: Key) -> Generator[Notification, None, None]:
        return self._store.getRange(offset, self._key, index=Base.BY_TIME_INDEX)


class LocalStorage:
    """ LocalStorage creates local storage for a given deviceID. Internally a partition is created
        using that deviceID and a timestamp of 0. All notifications are stored using time=0. """
    
    def __init__(self, st: Store, deviceID: str):
        self._store = st
        self._deviceID = deviceID
        key = Key(deviceID, 0, None, None)
        if not self._store.hasPartition(key):
            self._store.createPartition(key)
        self._key = key

    def store(self, ns: List[Notification]):
        notifs = []
        for n in ns:
            key = Key(self._deviceID, 0, n.key.path, n.key.key)
            notifs.append(Notification(key, n.value, n.meta))
            
            # enforce creation of a partition for every path which allows us to get keys
            key = Key(self._deviceID, 0, n.key.path, None)
            if not self._store.hasPartition(key):
                self._store.createPartition(key)

        self._store.store(notifs)

    def storeLocal(self, path: str, key: bytes, value: object):
        self.store([Notification(Key(self._key.device_id, 0, path, key), value)])

    def get(self, key: Key) -> Notification:
        return self._store.get(Key(key.device_id, 0, key.path, key.key))

    def getLocal(self, path: str, key: bytes) -> object:
        return self._store.get(Key(self._key.device_id, 0, path, key))

    def getRangeLocal(self, path: str) -> Generator[Notification, None, None]:
        key = Key(self._key.device_id, 0, path, None)
        return self._store.getRange(key, key)


def testQueue() -> int:
    # test queue generates offset correctly

    pass

def testLocalStorage() -> int:
    pass

if __name__ == '__main__':
    import sys
    sys.exit(testQueue() + testLocalStorage())