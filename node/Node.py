import time
import network
import socket
import uasyncio as asyncio

from message.Message import (new_message, ACTION_WRITE, DEST_BROADCAST,
    ACTION_RECEIVED, DEST_LOCAL, DEST_UPLINK, DEST_NODE, unmarshall_JSON)
from message.Recorder import MessageRecorder
from store.Store import Store
from store.Key import Key

STORE_PATH = '/data'

# A default of one day may not be enough since it could take more than a single day
# to notice and fix a network partition. Hopefully we can make storage nodes cheap.
DEFAULT_MESSAGE_EXPIRY = 5*24*3600

def path(*args) -> str:
    return '/'.join(args)

class Node:
    """ Node represents a node of the network """
    def __init__(self):

        
        self.ap_if = ap = network.WLAN(network.AP_IF)
        self.sta_if = sta = network.WLAN(network.STA_IF)
        ap.active(True)
        sta.active(True)

        from ubinascii import hexlify
        self.device_id = hexlify(ap.config('mac'))

        self.store = MessageRecorder(STORE_PATH, self.device_id, DEFAULT_MESSAGE_EXPIRY)
        
        # uplink stores the IP of the data uplink
        try:
            _, uplink = self.store.latest('/local/config/uplink')
            self.uplink = str(uplink, 'utf-8')
        except ValueError:
            self.uplink = None
        print("set uplink to:", self.uplink)

        self.configure_ap()
        self.configure_sta()

        self.rtc_synced = False
        if sta.isconnected():
            # Initialize RTC to the current time
            from ntptime import settime
            try:
                settime()
                self.rtc_synced = True
            except OSError:
                pass
        if not self.rtc_synced:
            print("running in limited mode until RTC is set")

    def configure_ap(self):
        did = str(self.device_id, 'utf-8')
        # Set up AP mode
        try:
            essid_path = path('/config', did, 'ap', 'essid')
            _, essid = self.store.latest(essid_path)
        except ValueError:
            essid = b'MESH-NODE-' + self.device_id
            self.write_broadcast(essid_path, essid)

        try:
            # First check if there is a global value set for all nodes
            _, passwd = self.store.latest(path('/config', 'ap', 'passwd'))
        except ValueError:
            # Otherwise check if there is a value stored for this node
            try:
                passwd_path = path('/config', did, 'ap', 'passwd')
                _, passwd = self.store.latest(passwd_path)
            except ValueError:
                # Finally set a default password and record it.
                passwd = b'CHANGEMELATER'
                self.write_broadcast(passwd_path, passwd)

        # TODO: set up ifconfig

        self.ap_if.config(essid=essid, password=passwd)

    def configure_sta(self, force_scan=False):
        # Set up STA mode
        sta_essid, sta_passwd = '', ''
        essid_path = path('/local', 'config', 'sta', 'essid')
        passwd_path = path('/local', 'config', 'sta', 'passwd')

        if not force_scan:
            try:   
                _, sta_essid = self.store.latest(essid_path)
            except:
                pass

            try:
                _, sta_passwd = self.store.latest(passwd_path)
            except:
                pass

        if sta_essid and sta_passwd:
            self.sta_if.connect(sta_essid, sta_passwd)
            timeout = 0
            while not self.sta_if.isconnected() and timeout < 10:
                time.sleep_ms(1000)
                timeout += 1

        else:
            # scan and sort by rssi
            scan = self.sta_if.scan()
            scan.sort(key=lambda s: -s[3])
            for s in scan:
                bssid = s[0]
                if bssid.startswith(b'MESH-NODE'):
                    did = bssid.split(b'-')[-1]
                    node_id = str(did, 'utf-8')
                    if did == self.device_id:
                        continue
                    try:
                        _, passwd = self.store.latest(path('/config', node_id, 'ap', 'passwd'))
                        self.sta_if.connect(bssid, passwd)
                        timeout = 0
                        for _ in range(10):
                            time.sleep_ms(1000)
                            if self.sta_if.isconnected():
                                break
                        else:
                            continue

                        # Successfully connected, so put the config into storage
                        self.write_local(essid_path, bssid)
                        self.write_local(passwd_path, passwd)
                        break

                    except ValueError: # Config for AP not in storage
                        print("missing password for AP", bssid)
                        pass

    def write_local(self, path: str, value: bytes):
        msg = new_message(path, self.device_id, value, ACTION_WRITE, DEST_LOCAL)
        self.store.record(msg)

    def write_broadcast(self, path: str, value: bytes):
        msg = new_message(path, self.device_id, value, ACTION_WRITE, DEST_BROADCAST)
        self.store.record(msg)

    async def _send_messages(self):
        interval = 0
        while True:
            await asyncio.sleep(10 - interval)
            start_time = time.time()

            if self.uplink:
                parent = self.uplink
            else:
                parent = self.sta_if.ifconfig()[3]

            print("<<< Connecting to send messages to parent:", parent)
            success = False
            s = socket.socket()
            s.settimeout(10)
            s.setblocking(False)
            try:
                s.connect(socket.getaddrinfo(parent, 1337)[0][-1])

                for key, msg in self.store.meta_table().db.items():
                    s.send(msg + b'\r\n')
                    print('sent message:', key)

                s.close()
                success = True
            except OSError as e:
                print("could not connect to parent:", e)

            # clear our uplink messages and record a "received" repsonse for each one
            if success and self.uplink:
                for raw_msg in self.store.meta_table().db.values():
                    print(raw_msg)
                    msg = unmarshall_JSON(raw_msg)
                    if (msg.dest == DEST_UPLINK):
                        msg.action = ACTION_RECEIVED
                        self.store.record(msg)
            
            interval = time.time() - start_time

    async def _receive_messages(self):
        # bind to listen for incomming connections
        addr = socket.getaddrinfo('0.0.0.0', 1337)[0][-1]
        lis = socket.socket()
        lis.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lis.settimeout(10)
        lis.setblocking(False)
        lis.bind(addr)
        lis.listen(1)

        try:
            while True:
                print("<<< Try to receive messages from parent")
                try:
                    cl, addr = lis.accept()
                    print('client connected from', addr)
                    f = cl.makefile('rb', 0, newline='\r\n')
                    while True:
                        line = f.readline()
                        try:
                            msg = unmarshall_JSON(line)
                            self.store.record(msg)
                        except:
                            print('error: bad message:', line)

                    cl.close()
                except OSError:
                    pass

        finally:
            lis.close()    

    def listen_and_serve(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self._send_messages())
        loop.create_task(self._receive_messages())
        loop.run_forever()
