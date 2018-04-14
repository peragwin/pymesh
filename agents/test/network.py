STA_IF = 0
AP_IF = 1

class Node:
    def __init__(self, node_id: str, x: int, y: int):
        self.node_id = node_id
        self.coord = (x, y)

class NodeGroup:
    def __init__(self, nodes):
        self.nodes = {}
        for n in nodes:
            self.nodes[n.node_id] = n

    def signal_strength(self, node_a: str, node_b: str):
        a = self.nodes[node_a]
        b = self.nodes[node_b]

        ax, ay = a.coord
        bx, by = b.coord

        d = ((ax-bx)**2 + (ay-by)**2)**.5
        return -d

NODES = [
    Node('a', 0, 0),
    Node('b', 5, 2),
    Node('c', 1, 3),
    Node('d', 9, 3),
    Node('e', 5, 5),
    Node('f', 4, 6),
    Node('g', 7, 7),
    Node('h', 3, 8),
    Node('i', 10,10),
]

NODE_GROUP = NodeGroup(NODES)

class WLAN:
    def __init__(self, intf=STA_IF):
        self.intf = intf
        self.node_id = ''

    def config(self, essid: bytes, passwd: bytes):
        print("@@@ config", essid, passwd)
        self.node_id = str(essid.split(b'-')[-1], 'utf-8')

    def connect(self, essid: bytes, passwd: bytes):
        node_id = str(essid, 'utf-8').split('-')[-1]
        if node_id in NODE_GROUP.nodes:
            print("connection to node %s" % essid)
            self._connected = True
        else:
            print("connection to uplink", essid)
            self._connected = True


    def scan(self):
        return [(bytes('MESH-NODE-'+node.node_id, 'utf-8'), '', '', NODE_GROUP.signal_strength(self.node_id, node.node_id))
                for node in NODES]

    def active(self, a: bool):
        self._active = a
        if not a:
            self._connected = False

    def isconnected(self) -> bool:
        return self._connected    