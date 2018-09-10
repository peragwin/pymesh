
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

    def getPath(self, path: str, key: str = None) -> Generator[Notification, None, None]:
        k = Key(self._key.device_id, None, path, key)
        return self._store.getRange(k, k, index=Base.BY_PATH_INDEX)

    def getFromOffset(self, offset: Key) -> Generator[Notification, None, None]:
        return self._store.getRange(offset, self._key, index=Base.BY_TIME_INDEX)


class Device(Queue):
    """ Device creates a Queue for a given deviceID """
    
    def __init__(self, st: Store, deviceID: str):
        super().__init__(st, Key(deviceID, None, None, None))


def test() -> int:
    # test queue generates offset correctly

    pass

if __name__ == '__main__':
    import sys
    sys.exit(test())