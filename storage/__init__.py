

try:
    from typing import Generator, Tuple
except:
    pass

from storage.Key import Key

class Base:
    """ Base is the interface that any basic storage driver must implement """

    def read(self, key: Key) -> bytes:
        raise NotImplementedError
    
    def write(self, key: Key, value: bytes):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def getRange(self, start: Key, end: Key, reverse: bool = False) \
        -> Generator[Tuple[Key, bytes], None, None]:
        raise NotImplementedError
