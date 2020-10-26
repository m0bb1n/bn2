import sys
import time
from twisted.internet.protocol import Protocol, ClientFactory
import json
import os
from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_CRIT_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG, safe_copy_route_meta
from bn2.comms import decoder

from datetime import datetime, timedelta
import traceback


START_DEL = '\n\r'
END_DEL = '\r\n'


class BotClientProtocol(Protocol):
    factory = None
    connected = False
    def __init__(self, factory):
        self.factory = factory
        self.dbuffer = ""

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
        #self.transport.loseConnection()

    def send_to_inbox(self, data):
            msg = None
            try:
                msg =  data
                self.factory.log.debug("inbox << {}".format(msg['route']))

            except TypeError as e:
                self.factory.log.critical("error {} with {}:\n{}\n".format(e, type(data), data))
                raise e

            else:
                try:
                    self.factory.driver.inbox.put(msg, INBOX_SYS_MSG)
                except:
                    exit()

    def connectionMade(self):
        self.connected = True
        self.factory.log.debug("Connection made {} my uuid: {}".format(self.transport.getPeer(), self.factory.uuid))
        msg = create_local_task_message(
            'bd.@md.slave.connect',
            { 'uuid': self.factory.uuid }
        )

        self.send(json.dumps(msg))

        msg1 = self.factory.driver.create_local_task_message(
            'bd.@sd.master.connected',
            {}
        )

        self.factory.driver.inbox.put(msg1, INBOX_SYS_CRIT_MSG)


    def connectionLost(self, s):
        self.connected = False
        self.factory.log.debug("connection lost with master")
        msg1 = self.factory.driver.create_local_task_message(
            'bd.@sd.master.disconnected',
            {}
        )

        self.factory.driver.inbox.put(msg1, INBOX_SYS_CRIT_MSG)

    def send(self, payload):
        self.transport.write ((START_DEL+payload+END_DEL).encode('utf-8'))


class BotClientFactory(ClientFactory):
    protocol = BotClientProtocol

    def __init__(self, driver, uuid, log):
        self.driver = driver
        self.driver_inbox = driver.inbox
        self.uuid = uuid
        self.err_time = None
        self.log = log
        self.log.set_path('bd.@sd.comms', check=False)
        self.DISCONNECTED_WARNING = False

    def send_it(self, payload):
        if hasattr(self, 'p'):
            self.log.debug("OUTBOX >> {} callback[{}]...".format(payload['route'], payload['route_meta']['callback']))
            meta = {}
            safe_copy_route_meta(payload['route_meta'], meta, exclude=['msg_id', '_time_received'])
            payload['route_meta'] = meta
            payload = json.dumps(payload, default=lambda o: str(o))
            self.p.send(payload)
            self.DISCONNECTED_WARNING = False

        else:
            if not self.DISCONNECTED_WARNING:
                msg1 = self.driver.create_local_task_message(
                    'bd.@sd.master.disconnected',
                    {}
                )

                self.driver_inbox.put(msg1, INBOX_SYS_CRIT_MSG)
                self.DISCONNECTED_WARNING = True


    def buildProtocol(self, addr):
        p = BotClientProtocol(self)
        self.p = p
        return p

    def startedConnecting(self, connector):
        destination = connector.getDestination()
        self.log.debug("Started trying to connect to Master @ {}:{}".format(destination.host, destination.port))


    def clientConnectionLost(self, connector, reason):
        if hasattr(self, 'p'):
            self.p.connected = False

        self.log.error("LOST CONNECTION {}".format(reason))
        try:
            self.driver.sleep(2000)
            connector.connect()
        except:
            connector.disconnect()
            msg1 = self.driver.create_local_task_message(
                'bd.@sd.master.disconnected',
                {}
            )

            self.driver_inbox.put(msg1, INBOX_SYS_CRIT_MSG)


    def clientConnectionFailed(self, connector, reason):
        if hasattr(self, 'p'):
            self.p.connected = False

        self.log.critical("CONNECTION FAILED {}".format(reason))
        try:
            self.driver.sleep(2000)
            connector.connect()
        except:
            connector.disconnect()
