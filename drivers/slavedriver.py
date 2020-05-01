from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, OUTBOX_TASK_MSG, OUTBOX_SYS_MSG
from bn2.comms.client import BotClientFactory
from twisted.internet import reactor, task
from bn2.drivers.botdriver import BotDriver
from datetime import datetime



class SlaveDriver (BotDriver):
    model_tag = 'SDV1'
    def __init__(self, config):
        super(SlaveDriver, self).__init__(config)
        self.master_server_ip = 'localhost'
        self.master_server_port = 5500
        self.RUNNING_GLOBAL_TASK = False


        self.__bd_sd_route_mappings = {
            'bd.@sd.slave.auth': self.bd_sd_slave_auth,
            'bd.@sd.master.connected': self.bd_sd_master_connected,
            'bd.@sd.master.disconnected': self.bd_sd_master_disconnected,

            'bd.@sd.task.global.start': self.bd_sd_task_global_start,
            'bd.@sd.task.global.stop': self.bd_sd_task_global_stop
        }

        self.add_check_func(self.send_pulse_to_master, 1000*10)

        self.add_route_mappings(self.__bd_sd_route_mappings)

    def bd_sd_slave_auth(self, data, route_meta):
        #do some shit
        msg = create_local_task_message (
            'bd.@md.slave.register',
            {
                'uuid': self.uuid,
                'model_tag': self.model_tag
            }
        )
        self.send_message_to_master(msg, priority=OUTBOX_SYS_MSG)
        #self.send_msg

    def  bd_sd_master_connected(self, data, route_meta):
        pass

    def bd_sd_master_disconnected(self, data, route_meta):
        pass

    def run_global_task(self, data, route_meta):
        task_id = route_meta['task_id']
        route_meta['route'] = data['route']

        start_msg = create_local_task_message(
            'bd.@md.slave.task.started',
            {
                'task_id': task_id,
                'time_started': str(datetime.utcnow())
            }
        )

        self.send_message_to_master(start_msg)

        self.log.info("Starting Global Task [{}]".format(route_meta['task_id']), path='bd.@sd.task.global.handler')

        err, msg = self.router(data['route'], data['data'], route_meta)

        end_msg = None
        if err:
            end_msg = create_local_task_message(
                'bd.@md.slave.task.error',
                {
                    'task_id': task_id,
                    'msg': msg
                }
            )
            #print error here

        else:
            end_msg = create_local_task_message(
                'bd.@md.slave.task.completed',
                {
                    'task_id': task_id,
                    'time_completed': str(datetime.utcnow())
                }
            )
            self.log.info("Completed Global Task [{}]".format(route_meta['task_id']), path='bd.@sd.task.global.handler')
        self.send_message_to_master(end_msg)


    def bd_sd_task_global_start(self, data, route_meta):
        if not self.RUNNING_GLOBAL_TASK:
            self.RUNNING_GLOBAL_TASK = True
            self.RUNNING_TASK_ID = route_meta['task_id']
            self.RUNNING_JOB_ID = route_meta['job_id']

            self.run_global_task(data, route_meta)

#            self.router(data['route'], data['data'], route_meta)

            self.RUNNING_GLOBAL_TASK = False
            self.RUNNING_TASK_ID = None
            self.RUNNING_JOB_ID = None
            pass
        else:
            self.log.error("Rejecting Task [{}] from Master because already running Task [{}]".format(route_meta['task_id'], self.RUNNING_TASK_ID))
            self.reject_global_task(route_meta['task_id'], route_meta['job_id'])


    def bd_sd_task_global_stop(self, data, route_meta):
        pass

    def reject_global_task(self, task_id, job_id):
        msg = create_local_task_message(
            'bd.@md.slave.task.reject',
            {
                'task_id': task_id,
                'job_id': job_id
            }
        )
        self.send_message_to_master(msg)

    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        data = {'target_uuid': uuid, 'data':msg}
        out = create_local_task_message('bd.@md.bot.message.send', data)
        self.outbox.put(out, priority)

    def send_pulse_to_master(self):
        if self.uuid:
            data = {'uuid': self.uuid}
            out = create_local_task_message('bd.@md.slave.pulse', data)
            self.send_message_to_master(out)

    def send_message_to_master(self, data, priority=OUTBOX_TASK_MSG):
        self.outbox.put(data, priority=OUTBOX_TASK_MSG)

    def run_comms(self):
        self.heartbeat.__track_process__(name='SlaveDriver Comms', route='@bd.comms.launch')
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse)

        cp = self.comms_pulse_cb.start(10)
        self.init_outbox_callback()

        self.factory = BotClientFactory(self.uuid, self.inbox, self.log)


        reactor.connectTCP(self.master_server_ip, self.master_server_port, self.factory)
        reactor.run()

