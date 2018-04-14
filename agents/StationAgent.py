try:
    import network
except:
    import agents.test.network as network
import json
import time

from message.Agent import Agent
from message import Broker
from message import ACTION_WRITE
from message.Message import Message
from agents import NETWORK_SEND_HELLO, network_set_parent

ESSID_PATH = '/system/sta/essid'
PASSWD_PATH = '/system/sta/passwd'
RECONFIGURE_PATH = '/system/sta/reconfigure'
CONNECT_PATH = '/system/sta/connect'

class StationAgent(Agent):
    """ StationAgent controls the operation of the WLAN in STA mode """
    
    path = '/system/sta'

    def __init__(self, broker: Broker):
        super().__init__(broker)

        self.sta_if = network.WLAN(network.STA_IF)
        self.sta_if.active(True)
        
        # FIXME: remove after testing
        self.sta_if.config(bytes(broker.node_id, 'utf-8'), b'')
        
        self.configure()

    def handler(self, msg: Message):
        if msg.action == ACTION_WRITE:
            if msg.path == RECONFIGURE_PATH:
                hops = json.loads(str(msg.value, 'utf-8'))
                self.reconfigure(hops)

            if msg.path == CONNECT_PATH:
                d = json.loads(str(msg.value, 'utf-8'))
                node_id = d.get('n', '')
                if node_id:
                    essid = self.read('/config/'+node_id+'/ap/essid')
                    passwd = self.read('/config/'+node_id+'/ap/passwd')
                else:
                    essid = d.get('e', '')
                    passwd = d.get('p', '')
                if essid and passwd:
                    self.connect(essid, passwd=passwd)
                else:
                    print("@@@ error cant connect:", d, essid, passwd)

    def scan(self):
        # scan and sort by rssi
        scan = self.sta_if.scan()
        scan.sort(key=lambda s: -s[3])
        ret = []
        for s in scan:
            bssid = s[0]
            if not bssid.startswith(b'MESH-NODE'):
                continue
            did = bssid.split(b'-')[-1]
            node_id = str(did, 'utf-8')
            if node_id == self.node_id:
                continue
            ret.append((bssid, node_id))
        return ret

    def connect(self, bssid: bytes, node_id: str = '', passwd: bytes = b''):
        if not passwd:
            passwd = self.read('/config/' + node_id + '/ap/passwd') or b'CHANGEMELATER'

        self.sta_if.connect(bssid, passwd)
        for _ in range(10):
            if self.sta_if.isconnected():
                break
            time.sleep(1)
        else:
            return False

        self.write_local(ESSID_PATH, bssid)
        self.write_local(PASSWD_PATH, passwd)
        if node_id:
            self.write_local('/system/network/parent', network_set_parent(node_id))
        else:
            self.write_local('/system/network/parent', network_set_parent('UPLINK'))
        self.write_local('/system/network', NETWORK_SEND_HELLO)
        return True

    def disconnect(self):
        self.sta_if.active(False)
        self.write_local('/system/network/parent', network_set_parent(''))
    
    def configure(self, force_scan=False):
        # Set up STA mode
        sta_essid, sta_passwd = '', ''

        if not force_scan:
            sta_essid = self.read(ESSID_PATH)
            sta_passwd = self.read(PASSWD_PATH)

        if sta_essid and sta_passwd:
            self.connect(sta_essid, passwd=sta_passwd)

        else:
            scan = self.scan()
            for bssid, node_id in scan:
                if self.connect(bssid, node_id=node_id):
                    return

    def reconfigure(self, hops: list):
        hopSet = {h:1 for h in hops}
        nodes = self.scan()
        for bssid, node_id in nodes:
            print(node_id, "in", hopSet)
            if node_id in hopSet:
                continue

            if self.connect(bssid, node_id=node_id):
                return
        else:
            self.disconnect()