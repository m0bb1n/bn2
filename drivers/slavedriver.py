from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, OUTBOX_TASK_MSG, OUTBOX_SYS_MSG
from bn2.comms.client import BotClientFactory
from twisted.internet import reactor, task
from bn2.drivers.botdriver import BotDriver, ASYNC_PRIORITY
from datetime import datetime
import os



class SlaveDriver (BotDriver):
    model_tag = 'sd'
    CONNECTED_TO_MASTER = False
    BOT_TYPE = 'SLAVE'

    bn2_public_key = None
    RUNNING_GLOBAL_TASK = False
    RUNNING_ROUTE = None
    RUNNING_TASK_ID = None
    RUNNING_JOB_ID = None

    route_levels = None

    def __init__(self, config):
        super(SlaveDriver, self).__init__(config)
        self._init_slave_configs()
        self.BOT_TYPE = 'SLAVE'


        self.add_route_mapping('bd.@sd.auth', self.bd_sd_auth, is_async=True)

        self.add_route_mapping('bd.@sd.token', self.bd_sd_token, is_async=True)

        self.add_route_mapping('bd.@sd.route.levels.set', self.bd_sd_route_levels_set, is_async=True)
        self.add_route_mapping('bd.@sd.master.connected', self.bd_sd_master_connected, is_async=True)
        self.add_route_mapping('bd.@sd.master.disconnected', self.bd_sd_master_disconnected, is_async=True)
        self.add_route_mapping('bd.@sd.task.global.start', self.bd_sd_task_global_start, is_async=True)
        self.add_route_mapping('bd.@sd.task.global.stop', self.bd_sd_task_global_stop, is_async=True)
        self.add_route_mapping('bd.@sd.task.global.pids', self.bd_sd_task_global_pids, is_async=True, level="LOCAL")
        self.add_route_mapping('bd.@sd.master.pulse', self.bd_sd_master_pulse, is_async=True, level="LOCAL")
        self.add_route_mapping('bd.@sd.master.message', self.bd_sd_master_message, is_async=True, level="LOCAL")
        self.add_check_func(self.send_pulse_to_master, 1000*20)


        if self.get_bot_config('uuid'):
            self.uuid = self.get_bot_config('uuid')
            self.token = self.get_bot_config('token')


    def bd_sd_master_message(self, data, route_meta):
        self.send_message_to_master(data['msg'])

    def bd_sd_master_pulse(self, data, route_meta):
        odata = {'send_time': str(datetime.utcnow()), 'running_slave_task':self.RUNNING_GLOBAL_TASK}
        if self.RUNNING_GLOBAL_TASK:
            odata['running_slave_task_id'] = self.RUNNING_TASK_ID
            odata['running_slave_job_id'] = self.RUNNING_JOB_ID

        out = self.create_local_task_message('bd.@md.slave.pulse', odata)
        self.send_message_to_master(out)


    def _init_slave_configs(self):
        self.add_bot_config(
            'uuid',
            None,
            'text',
            default=None
        )

        self.add_bot_config(
            'token',
            'Route Token',
            'text',
            default=None
        )


        self.load_bot_config_from_file()



    def bd_sd_token(self, data, route_meta):
        #MAYBE LOCK HERE?
        token = data['token']
        self.token = token
        self.log.debug("Set token to: {}".format(self.token))


    def bd_sd_route_levels_set(self, data, route_meta):
        # put a lock HERE VERY NEEDED
        self.route_levels = data['route_levels']
        #RELEAE HERE


    def bd_sd_auth(self, data, route_meta):
        msg = None
        if not data.get('verify', False):
            self.log.debug("Storing bn2 public key and sending slave information to master.")
            pub = data['bn2_public_key']
            self.bn2_public_key = self.get_rsa_key_from_str(pub)
            msg = create_local_task_message (
                'bd.@md.slave.register',
                {
                    'uuid': self.uuid,
                    'model_tag': self.model_tag
                }
            )
        else:
            self.log.debug("Sending verification info to master")
            msg = self.create_local_task_message (
                'bd.@md.slave.verify',
                {}
            )

        self.send_message_to_master(msg, priority=OUTBOX_SYS_MSG)

    @staticmethod
    def DRIVER_EVENT_MASTER_CONNECTION(is_connected):
        #slavedrivers can overwrite this event
        pass

    def  bd_sd_master_connected(self, data, route_meta):
        self.CONNECTED_TO_MASTER = True
        self.DRIVER_EVENT_MASTER_CONNECTION(True)

    def bd_sd_master_disconnected(self, data, route_meta):
        self.CONNECTED_TO_MASTER = False
        self.DRIVER_EVENT_MASTER_CONNECTION(False)

    def edit_global_job(self, job_id:int=None, **kwargs) -> None:
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise ValueError("job id is None")
            job_id = self.RUNNING_JOB_ID

        kwargs['job_id'] = job_id
        msg = self.create_local_task_message(
            'bd.@md.slave.job.edit',
            kwargs
        )



        self.send_message_to_master(msg)
        self.log.info("Editing Global Job [{}]: {}".format(job_id, kwargs), path='bd.@sd.job.global.edit')



    def run_global_task(self, data:dict, route_meta:dict) -> None:
        self.RUNNING_GLOBAL_TASK_PID = os.getpid()
        task_id = route_meta['task_id']
        job_id = route_meta['job_id']


        #print("ROUTE_META: {}\n DRM:{}\n\n".format(route_meta, data))


        start_msg = self.create_local_task_message(
            'bd.@md.slave.task.started',
            {
                'task_id': task_id,
                'time_started': str(datetime.utcnow())
            }
        )

        self.send_message_to_master(start_msg)

        self.log.info("Starting Global Task [{}]".format(route_meta['task_id']), path='bd.@sd.task.global.handler >> '+data['route'])

        route_meta['route'] = data['route'] #Set the running global task as route in meta
        err = self.router(data['route'], data['data'], route_meta)

        end_msg = None
        if err:
            end_msg = self.create_local_task_message(
                'bd.@md.slave.task.error',
                {
                    'task_id': task_id,
                    'msg':err,
                    'time_ended': str(datetime.utcnow())
                }
            )
            self.log.error("Error Global Task [{}]: {}".format(route_meta['task_id'], err))

        else:
            end_msg = self.create_local_task_message(
                'bd.@md.slave.task.completed',
                {
                    'task_id': task_id,
                    'time_ended': str(datetime.utcnow())
                }
            )
            self.log.info("Completed Global Task [{}]".format(route_meta['task_id']), path='bd.@sd.task.global.handler')

        stop_msg = self.create_local_task_message(
            'bd.@sd.task.global.stop',
            {"task_id": task_id}
        )
        self.inbox.put(stop_msg, INBOX_SYS_MSG)

        self.send_message_to_master(end_msg)



    def bd_sd_task_global_start(self, data, route_meta):
        reject_task = True
        if not self.RUNNING_GLOBAL_TASK:

            self.RUNNING_GLOBAL_TASK = True
            self.RUNNING_ROUTE = data['route']
            self.RUNNING_TASK_ID = route_meta['task_id']
            self.RUNNING_JOB_ID = route_meta['job_id']


            try:
                task_process = self.create_func_process(
                    self.run_global_task,
                    data,
                    route_meta
                )
                self.RUNNING_GLOBAL_TASK_PID = task_process.pid

            except Exception as e:
                self.log.error(e)
                self.RUNNING_GLOBAL_TASK = False
                self.RUNNING_ROUTE = None
                self.RUNNING_TASK_ID = None
                self.RUNNING_JOB_ID = None

            else:
                reject_task = False

        if reject_task:
            self.log.error("Rejecting Task [{}] from Master because already running Task [{}]".format(route_meta['task_id'], self.RUNNING_TASK_ID))
            self.reject_global_task(route_meta['task_id'], route_meta['job_id'])


    def bd_sd_task_global_stop(self, data, route_meta):
        if data['task_id'] == self.RUNNING_TASK_ID and self.RUNNING_GLOBAL_TASK:
            self.log.info("Stopping Task [{}] -- forced={}".format(self.RUNNING_TASK_ID, data.get('force', False)))


            if data.get('force', False):
                pids = []
                pids = self.global_task_info['pids']
                pids.append(self.RUNNING_GLOBAL_TASK_PID)
                if pids:
                    self.log.warning("Force stoping -- killing {} pids from global task: {}".format(len(pids), pids))
                for pid in pids:
                    msg = self.create_local_task_message(
                        "@bd.process.kill",
                        {"pid":pid}
                    )
                    self.inbox.put(msg, INBOX_SYS_CRITICAL_MSG)

            self.RUNNING_GLOBAL_TASK = False
            self.RUNNING_GLOBAL_TASK_PID = None
            self.RUNNING_TASK_ID = None
            self.RUNNING_JOB_ID = None


    def bd_sd_task_global_pids(self, data, route_meta):
        self.global_task_info['pids'].extend(data['pids'])


    def reject_global_task(self, task_id, job_id):
        msg = self.create_local_task_message(
            'bd.@md.slave.task.reject',
            {
                'task_id': task_id,
                'job_id': job_id
            }
        )
        self.send_message_to_master(msg)

    def set_job_report_keys(self, job_id=None, **kwargs):
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job_id = self.RUNNING_JOB_ID

        keys = []
        for key, val in kwargs.items():
            keys.append({"key":key, "value":val, 'action': 'set'})

        msg = self.create_local_task_message(
            'bd.@md.slave.job.report.keys.set',
            {"keys": keys, "job_id":job_id}
        )

        self.send_message_to_master(msg, OUTBOX_SYS_MSG)

    def set_job_report_key(self, key, val, job_id=None, add=False):
        action='set'
        if add:
            action = 'add'
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job_id = self.RUNNING_JOB_ID


        msg = self.create_local_task_message(
            'bd.@md.slave.job.report.key.set',
            {'key':key, 'value': val, 'job_id':job_id, 'action': action}
        )
        self.send_message_to_master(msg, OUTBOX_SYS_MSG)


    def create_job_report(self, skeleton, _type='plaintext', default='{}', job_id=None):
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job_id = self.RUNNING_JOB_ID


        ALLOWED_TYPES = ['plaintext', 'vue', 'html', 'json']
        if not _type in ALLOWED_TYPES:
            raise ValueError("Job report type has to be one of the following: {}".format(ALLOWED_TYPES))

        msg = self.create_local_task_message(
            'bd.@md.slave.job.report.create',
            {"job_id": job_id, 'type': _type, 'skeleton': skeleton, 'data': default}
        )
        self.send_message_to_master(msg, OUTBOX_SYS_MSG)


    def generate_job_report(self, job_id=None, pause=True):
        #can generate multiple times to update info throughout job
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job_id = self.RUNNING_JOB_ID

        msg = self.create_local_task_message(
            'bd.@md.slave.job.report.generate',
            {"job_id": job_id}
        )
        if pause:
            self.add_local_msg(
                self.create_local_task_message(
                    "bd.@sd.master.message",
                    {"msg":msg}
                ),
                delay=3
            )
            return
        self.send_message_to_master(msg)



    def query_WCv2(self, query, tag=None, job_id=None, name='Querying warehouse'):
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job_id = self.RUNNING_JOB_ID

        self.add_global_task('bd.sd.@WCv2.query', {'query':query, 'tag':tag}, name, job_id=job_id, job_ok_on_error=True)


    def insert_WCv2(self, table_name, row, job_id=None):
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job = self.RUNNING_JOB_ID

        rows = []
        if type(row) == list:
            rows = row
        else:
            rows = [row]


        payload = {'table_name': table_name, 'rows': rows}
        name = "Inserting {} rows in warehouse db table '{}'".format(len(rows), table_name)

        self.add_global_task('bd.sd.@WCv2.model.insert', payload, name, job_id=job_id)


    def update_WCv2(self, table_name, row, job_id=None, overwrite=False, name=None):
        if not job_id:
            if not self.RUNNING_JOB_ID:
                raise TypeError("job id is None")

            job = self.RUNNING_JOB_ID

        rows = []
        if type(row) == list:
            rows = row
        else:
            rows = [row]
        if not name:
            name = 'Modifying table "{}" in warehouse'.format(table_name)
        name+= ' [{} row(s)]'.format(len(rows))

        payload = {'table_name': table_name, 'rows':rows, 'overwrite': overwrite}
        self.add_global_task('bd.sd.@WCv2.model.update', payload, name, job_id=job_id)


    def add_global_tasks(self, route, tasks, job_id=None, **kwargs):
        if not job_id:
            job_id = self.RUNNING_JOB_ID
            if not job_id:
                raise TypeError("job id is None")

        task_data = []
        for data, name in tasks:
            if type(data) != dict:
                raise ValueError("data has to be a dict")

            task_data.append(self.create_slave_task_message(route, data, name, job_id=job_id, _only_task_info=True, **kwargs))

        msg = self.create_local_task_message (
            "bd.@md.slave.tasks.add",
            {"tasks":task_data}
        )

        self.outbox.put(msg, OUTBOX_SYS_MSG)

    def add_global_task(self, route, data, name, job_id=None, job_ok_on_error=False, retry_cnt=None, route_meta=None, create_job=False, timeout=None):
        if type(data) != dict:
            raise ValueError("data has to be a dict")
        if not job_id:
            meta  = route_meta
            if not meta:
                meta = {}


            job_id = self.RUNNING_JOB_ID
            if not job_id:
                job_id = meta.get('job_id', None)

            if (not self.RUNNING_JOB_ID or not job_id) and not create_job:
                raise TypeError("job id is None")


        msg = self.create_slave_task_message(
            route, data, name,
            job_id=job_id, job_ok_on_error=job_ok_on_error,
            retry_cnt=retry_cnt, route_meta=route_meta, timeout=timeout
        )
        self.outbox.put(msg, OUTBOX_SYS_MSG)


    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        data = {'uuid': uuid, 'msg':msg}
        out = self.create_local_task_message('bd.@md.slave.message.send', data)
        self.outbox.put(out, priority)

    def send_pulse_to_master(self):
        if self.token:
            self.bd_sd_master_pulse({}, {'local_route_secret':self.local_route_secret})
            #msg = self.create_local_task_message('bd.@sd.master.pulse', {})
            #self.add_local_msg(msg)

    def send_message_to_master(self, data, priority=OUTBOX_TASK_MSG):
        self.outbox.put(data, priority=priority)

    def run_comms(self):
        self.heartbeat.__track_process__(name='SlaveDriver Comms', route='@bd.comms.launch', relaunch=True)
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse)

        cp = self.comms_pulse_cb.start(10)
        self.init_outbox_callback()

        self.factory = BotClientFactory(self, self.uuid, self.log)

        reactor.connectTCP(self.get_bot_config('master_ip'), self.get_bot_config('master_port'), self.factory)
        reactor.run()

