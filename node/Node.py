import time
import network
import socket
# import uasyncio as asyncio
import _thread

from message.Message import (new_message, ACTION_WRITE, DEST_BROADCAST,
    ACTION_RECEIVED, DEST_LOCAL, DEST_UPLINK, DEST_NODE, unmarshall_JSON)
from message.Recorder import MessageRecorder
from store.Store import Store
from store.Key import Key

STORE_PATH = '/data'

# A default of one day may not be enough since it could take more than a single day
# to notice and fix a network partition. Hopefully we can make storage nodes cheap.
DEFAULT_MESSAGE_EXPIRY = 5*24*3600

DEFAULT_PASSWORD = b'CHANGEMELATER'

RUN_MODE_SLEEP = b'RM_SLP'
RUN_MODE_CONFIGURE_NETWORK = b'RM_CFG'

def path(*args) -> str:
    return '/'.join(args)

class Node:
    """ Node represents a node of the network """
    def __init__(self):

        self._th_recv_exit = False
        self._th_send_exit = False
        
        self.ap_if = ap = network.WLAN(network.AP_IF)
        self.sta_if = sta = network.WLAN(network.STA_IF)
        ap.active(True)
        sta.active(True)

        from ubinascii import hexlify
        self.device_id = hexlify(ap.config('mac'))

        self.store = MessageRecorder(STORE_PATH, self.device_id, DEFAULT_MESSAGE_EXPIRY)

        self.parent_node = self.read_str('/local/config/parent')
        self.run_mode = self.read_str('/local/runmode') or RUN_MODE_CONFIGURE_NETWORK

        # uplink stores the IP of the data uplink
        self.uplink = self.read_str('/local/config/uplink')
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
                passwd = DEFAULT_PASSWORD
                self.write_broadcast(passwd_path, passwd)

        # TODO: set up ifconfig

        self.ap_if.config(essid=essid, password=passwd)

    def configure_sta(self, force_scan=False):
        # Set up STA mode
        sta_essid, sta_passwd = '', ''
        essid_path = path('/local', 'config', 'sta', 'essid')
        passwd_path = path('/local', 'config', 'sta', 'passwd')

        if not force_scan:
            sta_essid = self.read(essid_path)
            sta_passwd = self.read(passwd_path)

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

    def _send_messages(self):
        interval = 0
        while True:
            if self._th_send_exit:
                return

            if self.run_mode == RUN_MODE_SLEEP:
                time.sleep(10)
                continue

            #await asyncio.sleep(10 - interval)
            time.sleep(max((10-interval, 0)))
            start_time = time.time()

            if self.uplink:
                parent = self.uplink
            else:
                parent = self.sta_if.ifconfig()[3]

            print("<<< Connecting to send messages to parent:", parent)
            success = False
            s = socket.socket()
            s.settimeout(10)
            #s.setblocking(False)
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

            # TODO: FILTER PARENT/CHILD FORWARDING BY DEST
            # TODO: SEND MESSAGES TO CHILDREN
            
            interval = time.time() - start_time

    def _receive_messages(self):
        # bind to listen for incomming connections
        addr = socket.getaddrinfo('0.0.0.0', 1337)[0][-1]
        lis = socket.socket()
        lis.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lis.settimeout(10)
        #lis.setblocking(False)
        lis.bind(addr)
        lis.listen(1)

        try:
            while True:
                if self._th_recv_exit:
                    return

                if self.run_mode == RUN_MODE_SLEEP:
                    time.sleep(10)
                    continue

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
        # loop = asyncio.get_event_loop()
        # loop.create_task(self._send_messages())
        # loop.create_task(self._receive_messages())
        # loop.run_forever()
        self._th_send = _thread.start_new_thread(self._send_messages, ())
        self._th_recv = _thread.start_new_thread(self._receive_messages, ())

    def exit(self):
        self.store.close()
        self._th_send_exit = True
        self._th_recv_exit = True
