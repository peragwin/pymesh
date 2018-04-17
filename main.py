#from node.Node import Node
import micropython

from board.proto0 import ProtoBoard
pb = ProtoBoard()

# node = Node()

# node.write_broadcast('/test', b'hello from the other side')

micropython.mem_info()

# node.listen_and_serve()