
from MessageStore import MessageStore

STORE_PATH = '/db'

# A default of one day may not be enough since it could take more than a single day
# to notice and fix a network partition. Hopefully we can make storage nodes cheap.
DEFAULT_MESSAGE_EXPIRY = 24*3600

class Node:
    """ Node represents a node of the network """
    def __init__(self):

        # Initialize RTC to the current time
        from ntptime import settime
        settime()
        from machine import RTC
        self._rtc = RTC()


        self.store = MessageStore(STORE_PATH, DEFAULT_MESSAGE_EXPIRY)

        if self.store.contains(
