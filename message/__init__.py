# Constants used throughout the message package

ACTION_WRITE = 1
ACTION_RECEIVED = 2
ACTION_REQUEST = 3
ACTION_RESPONSE = 4
ACTION_SNAPSHOT = 5

DEST_LOCAL = 0
DEST_BROADCAST = 1
DEST_UPLINK = 2
DEST_NODE = 4
DEST_PARENT = 8
DEST_CHILD = 16
DEST_NEIGHBORS = 24

SEND_TO_PARENT = 0
SEND_TO_CHILD = 1

from message.Broker import Broker
