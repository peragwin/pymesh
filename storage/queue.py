
try:
    from typing import List
except ImportError:
    pass

from storage.Store import Store
from storage.Key import Key
from storage.notification import Notification

class Queue:
    """ A Queue is a single partition of the storage. Storage can be partitioned
        arbitrarily by any marker, but care should be taken to choose a partition
        marker that will not result in overlapping queues within the same storage.
        An offset is stored which marks the timestamp of the most recently written
        item. """

    def __init__(self, st: Store, start: Key, end: Key):
        self._start = start
        self._end = end
        try:
            # try to get the most recent key written in this partition
            _ = self.getOffset()
        except StopIteration:
            # partition exists but has no entries, so offset for this queue is 0
            pass
        except KeyError:
            # partition doesn't exist yet, so we get to create it!
            st.store([
                Notification(start, None),
                Notification(end, None),
            ])

        self._store = st

    def getOffset(self) -> int:
        try:
            return next(self._store.getRange(self._start, self._end, reverse=True)).key.time
        except StopIteration:
            return 0

    def store(self, ns: List[Notification]):
        self._store.store(ns)
    


def test() -> int:
    # test queue generates offset correctly

    pass

if __name__ == '__main__':
    import sys
    sys.exit(test())