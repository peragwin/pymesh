
import json
try:
    from typing import Generator, Tuple, List
except:
    pass

from storage import Base
from storage.Key import Key
from storage.notification import Notification

class Store:
    """ A Store provides an interface for interacting with a storage driver using
        notifications and the given codec """

    def __init__(self, store: Base, codec = json):
        self._store = store
        self._codec = codec

    def _makeNotification(self, key: Key, raw: bytes) -> Notification:
        valmeta = self._codec.loads(raw)
        return Notification(key, valmeta['v'], valmeta['m'])

    def get(self, key: Key) -> Notification:
        return self._makeNotification(key, self._store.get(key))

    def store(self, ns: List[Notification]):
        for n in ns:
            self._store.store(n.key, self._codec.dumps({
                'v': n.value,
                'm': n.meta,
            }))

    def getRange(self, start: Key, end: Key, reverse: bool = False) \
        -> Generator[Notification, None, None]:
        for k, v in self._store.getRange(start, end, reverse):
            yield self._makeNotification(k, v)

    def createPartition(self, key: Key):
        """ createPartition for all keys that are prefixed with the given key """
        self._store.createPartition(key)

    def hasPartition(self, key: Key):
        """ hasPartition returns whether there is a partition for the given key in storage """
        self._store.hasPartition(key)
