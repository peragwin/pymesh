
from storage.Key import Key

# Possible orders for getRange
ASC = 0
DESC = 1

class Base:
    """ Base is the interface that any basic storage driver must implement """

    def read(self, key: Key) -> bytes:
        raise NotImplementedError
    
    def write(self, key: Key, value: bytes):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def getRange(self, start: Key, end: Key, order: int): # -> Generator[tuple]
        raise NotImplementedError
