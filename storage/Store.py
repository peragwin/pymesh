
import json
try:
    from typing import Generator, Tuple
except:
    pass

from storage import Base
from storage.Key import Key

class Store:
    """ A Store provides an interface for interacting with a storage driver """

    def __init__(self, store: Base, codec = json):
        self._store = store
        self._codec = codec

    def read(self, key: Key) -> object:
        return self._codec.loads(self._store.read(key))

    def write(self, key: Key, value: object):
        self._store.write(key, self._codec.dumps(value))

    def getRange(self, start: Key, end: Key, reverse: bool = False) \
        -> Generator[Tuple[Key, object], None, None]:
        for k, v in self._store.getRange(start, end, reverse):
            yield k, self._codec.loads(v)
