

try:
    from typing import Generator, Tuple
except:
    pass

from storage.Key import Key

class StorageException(Exception):
    pass

class PartitionAlreadyExistsError(StorageException):
    pass

class UnsupportedIndexError(StorageException):
    pass

class Base:
    """ Base is the interface that any basic storage driver must implement """

    BY_TIME_INDEX = '\x01'
    BY_PATH_INDEX = '\x02'

    def get(self, key: Key, index: chr) -> bytes:
        raise NotImplementedError
    
    def store(self, key: Key, value: bytes, index: chr = Base.BY_TIME_INDEX):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def getRange(self, start: Key, end: Key, reverse: bool = False, index: chr = Base.BY_TIME_INDEX) \
        -> Generator[Tuple[Key, bytes], None, None]:
        raise NotImplementedError

    def createPartition(self, key: Key):
        """ createPartition for all keys that are prefixed with the given key """
        raise NotImplementedError

    def hasPartition(self, key: Key):
        """ hasPartition returns true if the storage contains a partition for the given key """
        raise NotImplementedError
