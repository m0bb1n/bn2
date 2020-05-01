from twisted.internet import reactor, protocol

from datetime import datetime
import json
from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_MSG, INBOX_SYS_CRIT_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from bn2.comms import decoder

import traceback

DELI = chr(4)
START_DEL = '\n\r'
END_DEL = '\r\n'

class Payload (object):
    def __init__(self, data, code, error):
        self.data = data
        self.code = code
        self.error = error

    @staticmethod
    def load(json_data):
        if type(json_data) == str:
            json_data = json.loads(json_data)
        payload = Payload(
            json_data['data'],
            json_data['code'],
            json_data['error']
        )
        return payload

    def dump(self):
        return json.dumps({
            'data': self.data,
            'code': self.code,
            'error': self.error
        })

class Echo(protocol.Protocol):
    def __init__(self, factory):
        self.factory = factory
        self.uuid = None
        self.dbuffer = ""

    def connectionMade(self):
        self.connected = True

    def connectionLost(self, reason):
        self.factory.log.debug('</> (Lost {}) bc: {}'.format(self.uuid, reason))
        if self.uuid in self.factory.connections.keys():
            #delete uuid from connections
            try:
                self.factory.driver_inbox.put(create_local_task_message('bd.@md.slave.lost', {'uuid':self.uuid}),0)
            except:
                pass
            finally:
                del self.factory.connections[self.uuid]


    def dataReceived(self, data):

        try:
            self.dbuffer+=data.decode('ascii')

            ref_buffer = [self.dbuffer]
            extracted = decoder.check_buffer(ref_buffer)
            self.dbuffer = ref_buffer[0]
            if extracted:
                for e in extracted:
                    e_json = json.loads(e)
                    self.send_to_inbox(e_json)
        except Exception as e:
            self.factory.log.critical("DR ERROR: {}".format(e))
            print(traceback.print_exc())

        else:
            return


    def send_to_inbox(self, data):
        priority = INBOX_SYS_MSG
        if data['route'] == 'bd.@md.slave.connect': #create seperate func
            self.uuid = data['data']['uuid']
            self.factory.connections[data['data']['uuid']] = self

        elif data['route'] == 'bd.@md.Slave.CPV1.forwarded':
            priority = INBOX_SYS_CRIT_MSG

        else:
            if not self.uuid:
                self.transport.abortConnection()
                self.factory.log.error("SLAVE IS NOT REGISTER DISCONNECTING: {}".format(self.uuid))
                return

        data['route_meta']['origin'] = self.uuid
        msg = create_local_task_message(
            data['route'],
            data['data'],
            data['route_meta']
        )
        self.factory.driver_inbox.put(msg, priority)

    def send(self, payload):
        #if self.connected:
        self.transport.write((START_DEL+payload+END_DEL).encode('utf-8'))

    def forward(self, data):
        if self.connected:
            self.transport.write(data)

class BotServerFactory(protocol.Factory):
    def __init__(self, uuid, driver_inbox, log):
        self.connections = {}
        self.uuid = uuid
        self.driver_inbox = driver_inbox
        self.log = log
        self.log.set_path('bd.md.comms')

    def buildProtocol(self, addr):
        self.log.debug("connection by", addr)
        e = Echo(self)
        return e

    def send_it(self, payload):
        uuid = payload['target_uuid']
        data = None
        if type(payload) == dict:
            data = json.dumps(payload['data'])
        try:
            self.connections[uuid].send(data)
        except KeyError:
            self.driver_inbox.put(create_local_task_message('bd.@md.slave.lost', {'uuid':uuid}),0)
            if uuid in self.connections.keys():
                del self.connections[uuid]

            self.log.error("Slave UUID: {} DOESNT EXIST AS CONNECTION".format(uuid))

#reactor.listenTCP(5007, EchoFactory())
#reactor.run()

#s = S(payload_master)
