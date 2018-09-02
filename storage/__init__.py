
# Possible orders for getRange
ASC = 0
DESC = 1

class Base:
    """ Base is the interface that any basic storage driver must implement """

    def read(self, key: bytes) -> bytes:
        raise NotImplementedError
    
    def write(self, key: bytes, value: bytes):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def getRange(self, start: bytes, end: bytes, order: int): # -> Generator[tuple]
        raise NotImplementedError
