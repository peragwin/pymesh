import uhashlib
import time

from message.Message import new_message, ACTION_WRITE
from message.Recorder import MessageRecorder
from store.Store import Store
from store.util import Key

STORE_PATH = '/data'

# A default of one day may not be enough since it could take more than a single day
# to notice and fix a network partition. Hopefully we can make storage nodes cheap.
DEFAULT_MESSAGE_EXPIRY = 5*24*3600

def path(*args) -> str:
    return '/'.join(args)

class Node:
    """ Node represents a node of the network """
    def __init__(self):

        self.store = MessageRecorder(STORE_PATH, DEFAULT_MESSAGE_EXPIRY)
        self.local = Store(path(STORE_PATH) + 'local')

        ap = network.WLAN(network.AP_IF)
        ap.active(True)

        from ubinascii import hexlify
        self.device_id = hexlify(ap.config('mac'))
        did = str(self.device_id)

        # Set up AP mode
        try:
            _, essid = self.store.latest(path(STORE_PATH, 'config', did, 'ap', 'essid'))
        except:
            essid = b'MESH-NODE-' + self.device_id[8:]
            self.store.write()

        try:
            # First check if there is a global value set for all nodes
            _, passwd = self.store.latest(path(STORE_PATH, 'config', 'ap', 'passwd'))
        except:
            # Otherwise check if there is a value stored for this node
            try:
                passwd_path = path(STORE_PATH, 'config', did, 'ap', 'passwd')
                _, passwd = self.store.latest(passwd_path)
            except:
                # Finally set a default password and record it.
                passwd = b'CHANGEMELATER'
                self.store.record(new_message(passwd_path, self.device_id, passwd, ACTION_WRITE))

        # TODO: set up ifconfig

        ap.config(essid=essid)
        ap.config(passwd=passwd)

        # Set up STA mode
        try:
            essid_path = path(STORE_PATH, 'config', 'sta', 'essid')
            _, essid = self.local.latest(essid_path)
        except:
            pass

        try:
            passwd_path = path(STORE_PATH, 'local', 'sta', 'passwd')
            _, passwd = self.local.latest(passwd_path)
        except;
            pass

        self.local.close()

        sta = network.WLAN(network.STA_IF)
        sta.active(True)
        if essid and passwd:
            sta.connect(essid, passwd)
            timeout = 0
            while not sta.isconnected() and timeout < 10:
                time.sleep_ms(1000)
                timeout += 1

        if sta.isconnected():
            # Initialize RTC to the current time
            from ntptime import settime
            settime()
            self.rtc_synced = True
        else:
            self.rtc_synced = False
            print("running in limited mode until RTC is set")


