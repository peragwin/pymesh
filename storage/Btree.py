
try:
    from typing import Generator, Tuple
except:
    pass

from io import TextIOWrapper

from storage import Base
from storage import PartitionAlreadyExistsError, UnsupportedIndexError
from storage.Key import Key

DEFAULT_CACHE_SIZE = 128 * 1024 # 128 kB


class Btree(Base):
    """ Data implements a datastore. The default index is BY_TIME_INDEX but the driver
        also supports BY_PATH_INDEXing. """
    
    KEY_DELIMITER = '\x1C'
    START_PARTITION = '\x00'
    END_PARTITION = '\x1F'

    BY_TIME_INDEX = '\x01'
    BY_PATH_INDEX = '\x02'

    def __init__(self, btree, stream: TextIOWrapper, cache_size=DEFAULT_CACHE_SIZE):
        self._stream = stream
        self._btree = btree

        db = btree.open(stream, cachesize=cache_size)
        self.db = db

    def __del__(self):
        self.close()

    def close(self):
        self.db.flush()
        self.db.close()
        self._stream.close()

    # needs some kind of <deviceID>:"BYPATHINDEX":<path>:<key>:<timestamp> key format
    # but it also cant interfere with the queue reading off the latest offset and getRange
    # maybe works if we have <deviceID>:"BYTIMEINDEX":<path>:<key> format as well

    def _serialize_time_indexed_key(self, k: Key, partitioner: chr = '') -> bytes:
        bs = bytes(k.device_id, 'utf-8')
        if k.time is not None:
            bs += self.KEY_DELIMITER + bytes(str(k.time), 'utf-8')
        else:
            return bs + partitioner
        if k.path is not None:
            bs += self.KEY_DELIMITER + bytes(k.path, 'utf-8')
        else:
            return bs + partitioner
        if k.key is not None:
            bs += self.KEY_DELIMITER + bytes(k.key, 'utf-8')
        else:
            return bs + partitioner
        return bs

    def _deserialize_time_indexed_key(self, b: bytes) -> Key:
        k = b.split(self.KEY_DELIMITER)
        try:
            return Key(
                str(k[0], 'utf-8'), # deviceID
                int(k[1]) if len(k) > 1 else None, # time
                str(k[2], 'utf-8') if len(k) > 2 else None, # path
                str(k[3], 'utf-8') if len(k) > 3 else None, # key
            )
        except:
            raise ValueError("unable to deserialize key: " + str(b, 'utf-8'))

    def _serialize_path_indexed_key(self, k: Key, partitioner: chr = '') -> bytes:
        bs = bytes(k.device_id, 'utf-8')
        if k.path is not None:
            bs += self.KEY_DELIMITER + bytes(str(k.path), 'utf-8')
        else:
            return bs + partitioner
        if k.key is not None:
            bs += self.KEY_DELIMITER + bytes(k.key, 'utf-8')
        else:
            return bs + partitioner
        if k.time is not None:
            bs += self.KEY_DELIMITER + bytes(k.time, 'utf-8')
        else:
            return bs + partitioner
        return bs

    def _deserialize_path_indexed_key(self, b: bytes) -> Key:
        k = b.split(self.KEY_DELIMITER)
        try:
            return Key(
                str(k[0], 'utf-8'), # deviceID
                int(k[3]) if len(k) > 3 else None, # time is last
                str(k[1], 'utf-8') if len(k) > 1 else None, # path
                str(k[2], 'utf-8') if len(k) > 2 else None, # key
            )
        except:
            raise ValueError("unable to deserialize key: " + str(b, 'utf-8'))

    def _serialize_key(self, k: Key, partitioner: chr = '', index: chr = Btree.BY_TIME_INDEX) -> bytes:
        if index == self.BY_TIME_INDEX:
            return self._serialize_time_indexed_key(k, partitioner)
        elif index == self.BY_PATH_INDEX:
            return self._serialize_path_indexed_key(k, partitioner)
        else:
            raise UnsupportedIndexError

    def _deserialize_key(self, b: bytes, index: chr = Btree.BY_TIME_INDEX) -> Key:
        if index == self.BY_TIME_INDEX:
            return self._deserialize_time_indexed_key(b)
        elif index == self.BY_PATH_INDEX:
            return self._deserialize_path_indexed_key(b)
        else:
            raise UnsupportedIndexError

    def get(self, key: Key, index: chr = Btree.BY_TIME_INDEX, partitioner: chr = '') -> bytes:
        k = self._serialize_key(key, partitioner, index)
        return self.db[k]

    def store(self, key: Key, value: bytes, index: chr = Btree.BY_TIME_INDEX, partitioner: chr = ''):
        k = self._serialize_key(key, partitioner, index)
        self.db[k] = value

    def getRange(self, start: Key, end: Key, reverse: bool = False, index: chr = Btree.BY_TIME_INDEX) \
        -> Generator[Tuple[Key, bytes], None, None]:
        """ getRange returns an iterator with all key-value items between the `start` and `end` key.
            Start and end partition markers are suffixed if either key is incomplete. """
        
        s = self._serialize_key(start, self.START_PARTITION, index)
        e = self._serialize_key(end, self.END_PARTITION, index)

        if not reverse:
            items = self.db.items(s, e)
        else:
            items = self.db.items(s, e, self._btree.DESC)

        for k, v in items:
            yield (self._deserialize_key(k), v)

    def createPartition(self, key: Key, index: chr = Btree.BY_TIME_INDEX):
        """ createPartition for all keys that are prefixed with the given key """
        if self.hasPartition(key):
            raise PartitionAlreadyExistsError
        
        self.store(key, b'\x00', index, self.START_PARTITION)
        self.store(key, b'\x00', index, self.END_PARTITION)

    def hasPartition(self, key: Key, index: chr = Btree.BY_TIME_INDEX) -> bool:
        """ hasPartition returns true if the storage contains a partition for the given key """
        try:
            _ = self.get(key, index, self.START_PARTITION)
            return True
        except KeyError:
            return False

def main():
    class Mock(dict):
        DESC = 1
        def open(self, file, cache_size):
            pass
        def close(self):
            pass
        def flush(self):
            pass
    
    bt = Btree(Mock(), None)

    # test writes

    # test reads

    # test getRange

    # test close

    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())