from twisted.internet import reactor, stdio
import sys
import time
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.protocols import basic
import json
import os
from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_CRIT_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG
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
        except Exception as e:
            self.factory.log.critical("DR ERROR: {}".format(e))
            print(traceback.print_exc())

        else:
            return
        #self.transport.loseConnection()

    def send_to_inbox(self, data):
            msg = None
            try:
                msg = create_local_task_message(
                    data['route'],
                    data['data'],
                    data['route_meta']
                )
            except TypeError as e:
                self.factory.log.critical("error {} with {}:\n{}\n".format(e, type(data), data))
                raise e
            else:
                self.factory.driver_inbox.put(msg, INBOX_SYS_MSG)

    def connectionMade(self):
        self.connected = True
        self.factory.log.debug("Connection made {} my uuid: {}".format(self.transport.getPeer(), self.factory.uuid))
        msg = create_local_task_message(
            'bd.@md.slave.connect',
            { 'uuid': self.factory.uuid }
        )

        self.send(json.dumps(msg))

        msg1 = create_local_task_message(
            'bd.@sd.master.connected',
            {}
        )

        self.factory.driver_inbox.put(msg1, INBOX_SYS_CRIT_MSG)


    def connectionLost(self, s):
        self.connected = False
        self.factory.log.debug("connection lost with master")
        msg1 = create_local_task_message(
            'bd.@sd.master.disconnected',
            {}
        )

        self.factory.driver_inbox.put(msg1, INBOX_SYS_CRIT_MSG)

    def send(self, payload):
        self.transport.write((START_DEL+payload+END_DEL).encode('utf-8'))


class BotClientFactory(ClientFactory):
    protocol = BotClientProtocol

    def __init__(self, uuid, driver_inbox, log):
        self.client = None
        self.driver_inbox = driver_inbox
        self.uuid = uuid
        self.err_time = None
        self.log = log
        self.log.set_path('bd.sd.comms')


    def send_it(self, payload):
        if hasattr(self, 'p'):
            self.log.debug("Sending payload to master [{}]...".format(payload['route']))
            if type(payload) == dict:
                payload = json.dumps(payload)

            self.p.send(payload)
        else:
            msg1 = create_local_task_message(
                'bd.@sd.master.disconnected',
                {}
            )

            self.driver_inbox.put(msg1, INBOX_SYS_CRIT_MSG)


    def buildProtocol(self, addr):
        p = BotClientProtocol(self)
        self.p = p
        return p

    def startedConnecting(self, connector):
        destination = connector.getDestination()
        self.log.debug("<client> started Connecting destionation: {}".format(destination))


    def clientConnectionLost(self, connector, reason):
        if hasattr(self, 'p'):
            self.p.connected = False

        self.log.error("LOST CONNECTION {}".format(reason))
        try:
            time.sleep(2)
            connector.connect()
        except:
            connector.disconnect()

    def clientConnectionFailed(self, connector, reason):
        if hasattr(self, 'p'):
            self.p.connected = False

        self.log.critical("CONNECTION FAILED {}".format(reason))
        try:
            time.sleep(2)
            connector.connect()
        except:
            connector.disconnect()
