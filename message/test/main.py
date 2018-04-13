from message.Broker import Broker
from message.Message import Message
from message.Agent import Agent
import os, json, time

try:
    os.stat('/tmp/upy/mesh/data')
except:
    os.makedirs('/tmp/upy/mesh/data')
else:
    os.system('rm -rf /tmp/upy/mesh/data')
broker = Broker('/tmp/upy/mesh/data', 'testID', 30)

log = []

class TestAgent(Agent):
    path = '/test'
    def handler(self, msg: Message):
        log.append(msg.value)

agent = TestAgent(broker)
agent.write_local('/test', 'test local message')
agent.write_broadcast('/test', 'test broadcast message')

assert b'test local message' in log, log
assert b'test broadcast message' in log, log
values = [json.loads(str(v, 'utf-8'))['v'] for v in broker.meta_table().db.values()]
assert 'test broadcast message' in values, values
assert 'test local message' not in values, values

agent.write_local('/another/path', 'test another path')
time.sleep(1) # FIXME: we may need better than one second resolution on the time stamps
agent.write_local('/another/path', 'test another path again')

read = agent.read_str('/another/path')
assert read == 'test another path again', read

print("PASS")