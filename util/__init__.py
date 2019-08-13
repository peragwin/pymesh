
import time

def now() -> int:
    """ now returns the current timestamp in unix format with millisecond precision """
    return int(time.time() * 1000)