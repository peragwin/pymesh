
import json
try:
    from typing import Generator, Tuple, List
except:
    pass

from storage import Base
from storage import UnsupportedIndexError
from storage.Key import Key
from storage.notification import Notification

class Store:
    """ A Store provides an interface for interacting with a storage driver using
        notifications and the given codec. It creates both BY_TIME and BY_PATH indices. """

    def __init__(self, store: Base, codec = json):
        self._store = store
        self._codec = codec

    def _makeNotification(self, key: Key, raw: bytes) -> Notification:
        valmeta = self._codec.loads(raw)
        return Notification(key, valmeta['v'], valmeta['m'])

    def get(self, key: Key, index: chr = Base.BY_TIME_INDEX) -> Notification:
        return self._makeNotification(key, self._store.get(key, self._resolveIndex(index)))

    def store(self, ns: List[Notification]):
        for n in ns:
            self._store.store(n.key, self._codec.dumps({
                'v': n.value,
                'm': n.meta,
            }), self._store.BY_TIME_INDEX)
            self._store.store(n.key, '\x00', self._store.BY_PATH_INDEX)

    def _resolveIndex(self, index: chr) -> chr:
        if index == Base.BY_TIME_INDEX:
            return self._store.BY_TIME_INDEX
        elif index == Base.BY_PATH_INDEX:
            return self._store.BY_PATH_INDEX
        else:
            raise UnsupportedIndexError

    def getRange(self, start: Key, end: Key, reverse: bool = False, index: chr = Base.BY_TIME_INDEX) \
        -> Generator[Notification, None, None]:
        index = self._resolveIndex(index)
        for k, v in self._store.getRange(start, end, reverse, index):
            yield self._makeNotification(k, v)

    def createPartition(self, key: Key):
        """ createPartition for all keys that are prefixed with the given key """
        self._store.createPartition(key, self._store.BY_TIME_INDEX)
        self._store.createPartition(key, self._store.BY_PATH_INDEX)

    def hasPartition(self, key: Key):
        """ hasPartition returns whether there is a partition for the given key in storage """
        self._store.hasPartition(key)
