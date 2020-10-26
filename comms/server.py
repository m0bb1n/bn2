from twisted.internet import reactor, protocol

from datetime import datetime
import json
from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_MSG, INBOX_SYS_CRIT_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG, safe_copy_route_meta

from bn2.comms import decoder

from twisted.internet import task
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
        #self.factory.log.debug('</> (Lost {}) bc: {}'.format(self.uuid, reason))
        if self.uuid:
            out = self.factory.driver.create_local_task_message('bd.@md.slave.lost', {'uuid':self.uuid, 'err_msg':"lost connection"})
            self.factory.driver.inbox.put(out,0)

            if self.uuid in self.factory.connections.keys():
                del self.factory.connections[self.uuid]
            self.uuid = None
        self.connected = False


    def dataReceived(self, data):
        self.connected = True

        try:
            self.dbuffer+=data.decode('ascii')

            ref_buffer = [self.dbuffer]
            extracted = decoder.check_buffer(ref_buffer)
            self.dbuffer = ref_buffer[0]
            if extracted:
                for e in extracted:
                    e_json = json.loads(e)
                    self.send_to_inbox(e_json)
            else:
                self.factory.log.warning("Incomplete message buffer -- waiting...")

        except Exception as e:
            self.factory.log.critical("DR ERROR: {}".format(e))
            print(traceback.print_exc())

        else:
            return


    def send_to_inbox(self, data):
        priority = INBOX_SYS_MSG
        if not self.uuid:
            if data['route'] == 'bd.@md.slave.connect': #create seperate func
                uuid = data['data']['uuid']
                if uuid in self.factory.connections.keys():
                    if self.factory.connections[uuid].connected and self != self.factory.connections[uuid]:
                        self.transport.abortConnection()
                        self.factory.log.error("Blocked new connection due to using same uuid of another active slave [{}]".format(uuid))

                        return

                ip, port = self.transport.client
                data['data']['ip'] = ip
                data['data']['port'] = port
                self.uuid = uuid
                self.factory.connections[uuid] = self

            else:
            #if not self.uuid and self.uuid in self.factory.connections.keys():
                self.transport.abortConnection()
                #out = self.factory.driver.create_local_task_message('bd.@md.slave.lost', {'uuid':self.uuid})
                #self.factory.driver.inbox.put(out,0)
                self.factory.log.error("DOES NOT HAVE SLAVE CONNECTION: {} Could not send route {}".format(self.uuid, data['route']))
                return



        if data['route'] == 'bd.@md.slave.register' or data['route'] == 'bd.@md.slave.verify':
            ip, port = self.transport.client
            data['data']['ip'] = ip
            data['data']['port'] = port

        data['route_meta']['origin'] = self.uuid

        self.factory.driver.inbox.put(data, priority)

    def send(self, payload):
        #if self.connected:
        self.transport.write((START_DEL+payload+END_DEL).encode('utf-8'))

    def forward(self, data):
        if self.connected:
            self.transport.write(data)

class BotServerFactory(protocol.Factory):
    def __init__(self, driver, uuid, log):
        self.connections = {}
        self.uuid = uuid
        self.driver = driver
        self.driver_inbox = driver.inbox
        self.log = log
        self.log.set_path('bd.@md.comms', check=False)

        self.recent_error_uuids = []

        task.LoopingCall(self.clear_recent_error_uuids).start(30)

    def clear_recent_error_uuids(self):
        self.recent_error_uuids = []

    def buildProtocol(self, addr):
        e = Echo(self)
        return e

    def send_it(self, payload):
        uuid = payload['target_uuid']
        data = None
        meta = {}
        from_uuid = payload.get('from_uuid', None)

        cmd = payload.get('comm_cmd', None)
        if cmd:
            self.log.debug("Executing COMM CMD >{}<".format(cmd))
            if cmd == 'del_connection':
                try:

                    try:
                        self.connections[uuid].transport.abortConnection()
                    except:
                        pass
                    finally:
                        del self.connections[uuid]
                except:
                    pass
            elif cmd == 'warn_connection':
                #if connection is abusing comms or trying to access unauthorized route
                pass
            else:
                raise ValueError("Unknown comm_cmd '{}'".format(cmd))
            return

        if not payload.get('data'):
            print('\n\nsomething wrong here... [COMMS/server.py]')
            print(payload)

        if payload['data'].get('route_meta', None):
            safe_copy_route_meta(payload['data']['route_meta'], meta, exclude=['msg_id', '_time_received'])

        if from_uuid:
            meta['origin'] = from_uuid

        payload['data']['route_meta'] = meta
        data = json.dumps(payload['data'], default=lambda o: str(o))

        try:
            self.connections[uuid].send(data)
        except KeyError:
            if not uuid in self.recent_error_uuids:
                self.recent_error_uuids.append(uuid)
                out = self.driver.create_local_task_message('bd.@md.slave.lost', {'uuid':uuid, 'from_comms':True})
                self.driver_inbox.put(out,INBOX_SYS_MSG)
                self.log.error("Slave [{}] doesn't have an active connection -- Can't send route '{}'".format(uuid, payload['data']['route']))

