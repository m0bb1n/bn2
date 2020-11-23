from bn2.utils.msgqueue import \
    create_local_task_message, create_global_task_message, \
    create_local_route_meta, \
    INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, \
    INBOX_TASK2_MSG, OUTBOX_TASK_MSG, OUTBOX_SYS_MSG
from bn2.comms.server import BotServerFactory
from twisted.internet import reactor, task
from bn2.drivers.botdriver import BotDriver

import traceback
import json
from sqlalchemy import and_, or_, event
from bn2.db.db import DBWrapper
from bn2.db import masterdb
import os
import urllib.parse
from datetime import datetime, timedelta
import boto3

DELETE_RECORD_UPDATES = {
}

def after_delete(mapper, connection, target):
    tablename = mapper.class_.__tablename__
    lst = DELETE_RECORD_UPDATES.get(tablename, [])
    lst.append(target.id)
    DELETE_RECORD_UPDATES[tablename] = lst

event.listen(masterdb.UpdateBase, 'after_delete', after_delete, propagate=True)

OP_ROLE = "*"
CP_SLAVE_LEVEL_GROUP = ["DB_MASTER_READ", "SLAVE", "CP_SLAVE"]
SLAVE_LEVEL_GROUP = ["SLAVE"]
MASTER_LEVEL_GROUP = ["MASTER", "SLAVE", "DB_MASTER_WRITE", "DB_MASTER_READ"]

# maybe dont give cp any perms so required to use user_token and stop abuse
# make all query master_db routes in cpv2 route_guard()



ROUTE_LEVELS = [
    'NETWORK_SLAVE_ADD',
    'NETWORK_SLAVE_REMOVE',
    'JOB_WRITE',
    'JOB_READ',
    'DB_MASTER_WRITE',
    'DB_MASTER_READ',
    'DB_WAREHOUSE_WRITE',
    'DB_WAREHOUSE_READ',
    'CP_SLAVE'
]


OP_ROUTE_LEVEL = "OP"
SLAVE_ROUTE_LEVEL = "SLAVE"
MASTER_ROUTE_LEVEL = "MASTER"
USER_ROUTE_LEVEL = "USER"

# in the future make op route a join array of individual levels

PANINI_ROUTE_LEVEL = ''

class MasterDriver (BotDriver):
    model_tag = 'md'
    CPv2_alert_non_persist_id = 0

    BOT_TYPE = 'MASTER'

    def __init__(self, config):
        super(MasterDriver, self).__init__(config)
        self.BOT_TYPE = "MASTER"
        self._init_master_configs()
        self._launch_time = datetime.utcnow()

        self.add_route_mapping('bd.@md.user.add', self.bd_md_user_add, is_async=True)
        self.add_route_mapping('bd.@md.user.remove', self.bd_md_user_remove, is_async=True)

        self.add_route_mapping('bd.@md.user.verify', self.bd_md_user_verify, is_async=True)
        self.add_route_mapping('bd.@md.tag.send', self.bd_md_tag_send, is_async=True)

        self.add_route_mapping('bd.@md.network.destruct', self.bd_md_network_destruct, is_async=True, level=OP_ROUTE_LEVEL)
        self.add_route_mapping('bd.@md.slave.lost', self.bd_md_slave_lost, is_async=True, level=SLAVE_ROUTE_LEVEL)
        self.add_route_mapping('bd.@md.slave.connect', self.bd_md_slave_connect, is_async=True)
        self.add_route_mapping('bd.@md.slave.register', self.bd_md_slave_register, is_async=True)
        self.add_route_mapping('bd.@md.slave.verify', self.bd_md_slave_verify, is_async=True, level='*')
        self.add_route_mapping('bd.@md.slave.pulse', self.bd_md_slave_pulse, is_async=True, level=SLAVE_ROUTE_LEVEL)
        self.add_route_mapping('bd.@md.slave.kill', self.bd_md_slave_kill, is_async=True)
        self.add_route_mapping('bd.@md.slave.launch', self.bd_md_slave_launch, is_async=True)
        self.add_route_mapping('bd.@md.slave.message.send', self.bd_md_slave_message_send, is_async=True)
        self.add_route_mapping('bd.@md<Slave.CPv2.job.list', self.bd_md_IN_Slave_CPv2_job_list, is_async=True)

        self.add_route_mapping('bd.@md.slave.job.add', self.bd_md_slave_job_add, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.remove', None, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.edit', self.bd_md_slave_job_edit, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.completed', self.bd_md_slave_job_completed, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.stop', self.bd_md_slave_job_stop, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.error', self.bd_md_slave_job_error, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.pause', self.bd_md_slave_job_pause, is_async=True)

        self.add_route_mapping('bd.@md.slave.job.report.create', self.bd_md_slave_job_report_create, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.report.generate', self.bd_md_slave_job_report_generate)
        self.add_route_mapping('bd.@md.slave.job.report.key.set', self.bd_md_slave_job_report_key_set, is_async=True)
        self.add_route_mapping('bd.@md.slave.job.report.keys.set', self.bd_md_slave_job_report_keys_set, is_async=True)

        self.add_route_mapping('bd.@md.slave.task.add', self.bd_md_slave_task_add, is_async=True)
        self.add_route_mapping('bd.@md.slave.tasks.add', self.bd_md_slave_tasks_add, is_async=True)

        self.add_route_mapping('bd.@md.slave.task.remove', None)
        self.add_route_mapping('bd.@md.slave.task.error', self.bd_md_slave_task_error, is_async=True)
        self.add_route_mapping('bd.@md.slave.task.check', self.bd_md_slave_task_check, is_async=True)
        self.add_route_mapping('bd.@md.slave.task.started', self.bd_md_slave_task_started, is_async=True, level='SLAVE')
        self.add_route_mapping('bd.@md.slave.task.completed', self.bd_md_slave_task_completed, is_async=True, level='SLAVE')
        self.add_route_mapping('bd.@md.slave.task.reject', self.bd_md_slave_task_reject, is_async=True, level='SLAVE')
        self.add_route_mapping('bd.@md.slave.task.stopped', None)

        self.add_route_mapping('bd.@md.Slave.CPv2.redirected', self.bd_md_Slave_CPv2_redirected, is_async=True, level='CP_SLAVE')
        self.add_route_mapping('bd.@md<Slave.CPv2.resp.times', self.bd_md_IN_Slave_CPv2_resp_times, is_async=True, level="SLAVE")
        self.add_route_mapping('bd.@md<Slave.CPv2.master.configs', self.bd_md_IN_Slave_CPv2_master_configs, level="SLAVE")
        self.add_route_mapping('bd.@md>Slave.CPv2.master.configs', self.bd_md_OUT_Slave_CPv2_master_configs, is_async=True, level="SLAVE")

        self.add_route_mapping('bd.@md.slave.task.schedule.add', self.bd_md_slave_task_schedule_add, is_async=True)
        self.add_route_mapping('bd.@md.slave.task.schedule.remove', self.bd_md_slave_task_schedule_remove)


        self.add_route_mapping('bd.@md.slave.pulses.check', self.bd_md_slave_pulses_check, is_async=True, level='LOCAL')
        self.add_route_mapping('bd.@md.slave.tasks.check', self.bd_md_slave_tasks_check, is_async=True, level='LOCAL')


        self.add_check_func(self.check_slave_tasks, 1000)
        self.add_check_func(self.check_slave_schedules, 1000 * 5)
        self.add_check_func(self.check_slave_recent_resp_times, 1000 * 45)

        #self.add_check_func(lambda: self.inbox.put(self.create_local_task_message('bd.@md.slave.tasks.check', {}), INBOX_SYS_CRITICAL_MSG), 1000 * 3)

        #self.add_check_func(lambda: self.inbox.put(self.create_local_task_message('bd.@md.slave.pulses.check', {}), INBOX_SYS_MSG), 1000 * 45)

        self.add_check_func(self.check_delete_records, 1000*2)

        self.slaves_recent_resp_times = {}


        self.register_route_level(*ROUTE_LEVELS)
        self.register_route_level(OP_ROUTE_LEVEL)
        self.register_route_level(SLAVE_ROUTE_LEVEL)
        self.register_route_level(MASTER_ROUTE_LEVEL)
        self.register_route_level(USER_ROUTE_LEVEL)

        if self.get_bot_config('enable_log_upload'):
            #self.gofile = gofile.GofileClient()
            #self.add_check_func(self.upload_logs, 1000 * 30)
            self.log.error("GOFile Client is offline -- LOG UPLOAD DISABLED", path='bd.@md.init')



        priv = self.get_bot_config("bn2_private_key")
        pub = self.get_bot_config("bn2_public_key")
        if not (priv and pub):
            self.log.info("Generating RSA Key pairs...", path='bd.@md.init')
            pub, priv = self.generate_rsa_key_pair(as_str=True)
            self.set_bot_config("bn2_private_key", priv)
            self.set_bot_config("bn2_public_key", pub)


        self.bn2_public_key = self.get_rsa_key_from_str(pub)
        self.bn2_private_key = self.get_rsa_key_from_str(priv)

        self.token = self.create_route_token(self.uuid, SLAVE_ROUTE_LEVEL, MASTER_ROUTE_LEVEL, is_master=True)

        if self.get_bot_config("use_slave_host_ec2"):
            if not (self.get_bot_config("aws_api_key") and self.get_bot_config("aws_api_secret")):
                self.set_bot_config("use_slave_host_ec2", False)
                self.log.error("Cannot enable EC2 slave hosting if both api key and secret are not provided")
            else:

                region_name = 'us-east-2'
                self.log.info("Initializing ec2 resource -- {}".format(region_name))
                self.init_ec2_resource(region_name)


        self._init_master_db_tables_()



    def init_ec2_resource(self, region):
        self.ec2_resource = boto3.resource(
            'ec2',
            region_name=region,
            aws_access_key_id=self.get_bot_config("aws_api_key"),
            aws_secret_access_key=self.get_bot_config("aws_api_secret")
        )


    def _init_master_configs(self):
        self.add_bot_config(
            'bn2_private_key',
            'BN2 Private RSA Key',
            'text',
            desc='DO NOT MODIFY',
            grouping='networking'
        )

        self.add_bot_config(
            "is_only_lan",
            "Master only accepts connection from LAN slaves",
            "bool",
            default=True,
            grouping='networking'
        )

        self.add_bot_config(
            "bn2_lib_download_url",
            "BN2 Library download url",
            "url",
            default=None,
            grouping='aws'
        )

        self.add_bot_config(
            "use_slave_host_ec2",
            "Use EC2 to launch slaves",
            "bool",
            default=False,
            grouping='aws'
        )

        self.add_bot_config(
            "aws_ec2_image_id_slave",
            "Default EC2 image_id for slaves",
            "str",
            default='ami-0e82959d4ed12de3f', #ubuntu 18.04
            desc="""Recommended to use ubuntu 18.04 image id best performance -- Default is ubuntu 18.04""",
            grouping='aws'
        )

        self.add_bot_config(
            "aws_ec2_security_group_slave",
            "Default EC2 security group for slaves",
            "str",
            desc="""Default security group for slaves launched dynamically.
            To SSH into slaves, make sure to allow INBOUND connections on PORT 22.
            Found in "Network & Security" tab on the EC2 console.""",
            grouping='aws'
        )

        self.add_bot_config(
            "aws_ec2_key_pair_slave",
            "SSH key pair name for EC2 slaves",
            "str",
            desc='SSH key pair you created or imported. Found in "Network & Security" tab on the EC2 console.',
            grouping='aws'
        )

        self.add_bot_config(
            "slave_init_grace",
            "Slave initialization grace period (seconds)",
            "number",
            default=120,
            grouping='aws'
        )
        self.add_bot_config(
            "slave_idle_grace",
            "Slave idle grace period (seconds)",
            "number",
            default=60*5,
            desc="Max grace period between last worked on task",
            grouping='aws'
        )

        self.add_bot_config(
            'enable_log_upload',
            'Enable master log upload',
            'bool',
            default=True,
            desc='Allows you to view master logs in Control Panel',
            grouping='debug'
        )

        self.add_bot_config(
            'aws_api_key',
            'AWS API Key',
            'str',
            default='default api key',
            desc='The AWS api key you will find in your amazon accout',
            grouping='aws'
        )

        self.add_bot_config(
            'aws_api_secret',
            'AWS API Secret',
            'str',
            default='default api secret',
            desc='The AWS api secret (keep hidden!)',
            grouping='aws'
        )


        self.add_bot_config(
            'persistent_task_data',
            'Persistent task data',
            'bool',
            desc='Store task data after job ends.  Used to help debugging.',
            default=True,
            grouping='debug'
        )

        self.add_bot_config(
            'master_db_engine',
            'Master Database Engine',
            'str',
        )

        self.add_bot_config(
            'master_db_address',
            'Master Database Address/File path',
            'str'
        )




        self.load_bot_config_from_file()




    def _init_master_db_tables_(self):
        engine = self.get_bot_config('master_db_engine')
        address = self.get_bot_config('master_db_address')


        db_exists = False
        if engine == 'sqlite':
            db_exists = os.path.exists(address)


        self.master_db = DBWrapper(address, masterdb.Base, engine, create=not db_exists, scoped_thread=True)
        jobs = []
        with self.master_db.scoped_session() as session:
            if not db_exists:
                try:
                    args = [
                        {'name': 'Control Panel', 'model_tag': 'CPv2'},
                        {'name': 'Chaturbate Traffic Bot', 'model_tag': 'CBBv1'},
                        {'name': 'Discord Clerk', 'model_tag': 'DSCv1'},
                        {'name': 'Warehouse Clerk', 'model_tag': 'WCv2'}
                    ]
                    for a in args:
                        session.scoped.add_model(masterdb.SlaveType, a)
                    session.commit()

                except Exception as e:
                    self.log.error(e)
                    session.rollback()

            else:
                session.rollback()

            slaves = session.query(masterdb.Slave).filter(or_(masterdb.Slave.active==True, masterdb.Slave.working==True)).all()
            self.remove_slaves(slaves=slaves, send_kill_slave=False, send_comm_cmd=False, session=session)


            jobs = session.query(masterdb.SlaveJob)\
                .filter(
                    masterdb.SlaveJob.error==False,
                    masterdb.SlaveJob.completed==False
                )\
                .all()

        self.MASTER_DB_SLAVE_TYPE_CPV2_ID = session.query(masterdb.SlaveType.id).filter(masterdb.SlaveType.model_tag=="CPv2").first()[0]

        if len(jobs):
            self.log.debug("Master db stopping previous jobs ({})".format(len(jobs)), path='bd.@md.init')


        for job in jobs:
            self._slave_job_error(job_id=job.id, msg='Master exited abruptly before jobs completed')



        with self.master_db.scoped_session() as session:
            root = session.query(masterdb.User)\
                .filter(masterdb.User.username=="root")\
                .first()
            if not root:
                root_add_msg = self.create_local_task_message('bd.@md.user.add', {"username": "root", "password": "root", "levels": ["OP"]})
                self.add_start_up_route(root_add_msg, INBOX_SYS_MSG)

    def create_route_level_bitmask(self, *levels) -> int:
        bitmask =0

        for level in levels:
            bitmask |= self.get_route_level_bitmask(level)
        return bitmask

    def register_route_level(self, *levels) -> None:
        for level in levels:
            if level in self.route_levels:
                raise ValueError("route level '{}' is already registered".format(level))

            if len(self.route_levels)>64:
                raise Exception("Mask bitmask permission is 64 for big int")

            self.route_levels.append(level)

    def create_route_token(self, origin,  *levels,  bitmask=None, user_id=None, username=None, slave=None, slave_id=None, slave_uuid=None, is_master=False):
        if slave:
            slave_id = slave.id
            slave_uuid = slave.uuid

        if not (user_id or slave_uuid or slave_id ) and not is_master:
            raise TypeError("user_id, slave_id or slave_uuid has to be specified")


        if len(levels):
            if levels[0] != None:
                bitmask = self.create_route_level_bitmask(*levels)

        if not bitmask:
            raise ValueError("Need to include bitmask if no levels")

        payload = {
            'origin': origin,
            'bitmask':bitmask,
            'levels':levels
        }

        if is_master:
            payload['is_master'] = True

        if user_id:
            if username:
                payload['username'] = username
            payload['user_id'] = user_id

        if slave_uuid:
            payload['slave_uuid'] = slave_uuid
            #slave_id =

        if slave_id:
            payload['slave_id'] = slave_id

        return self.encode_payload(payload, self.bn2_private_key)

    def run_comms (self): #this is for masterdriver
        self.heartbeat.__track_process__(name='MasterDriver Comms', route='@bd.comms.launch', relaunch=True)
        self.factory = BotServerFactory(self, self.uuid, self.log)
        intf = "0.0.0.0"
        if self.get_bot_config("is_only_lan"):
            intf = "localhost"

        reactor.listenTCP(self.get_bot_config('master_port'), self.factory, interface=intf)
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse)
        cp = self.comms_pulse_cb.start(10)
        self.init_outbox_callback()
        reactor.run()

    def send_message_to(self, uuid, msg, from_uuid=None, priority=OUTBOX_TASK_MSG):
        payload = {'target_uuid':uuid, 'data':msg, 'from_uuid':from_uuid}
        try:
            self.outbox.put(payload, priority)
        except Exception as e:
            self.report_issue(str(e), 'bd.md.send_message_to', ERROR_LVL=True)


    def send_message_to_all(self):
        pass


    def alert_CPv2(self, msg, go_to=None, persist=False, color=None, slave_id=None, sids=[], slave_uuid=None,  slave_uuids=[], redirect_msg_id=None, redirect_resp=False, resp_error=False, resp_warning=False, resp_ok=False, throw_error_no_CPv2=False):
        id_ = 'np-'+str(self.CPv2_alert_non_persist_id)
        self.CPv2_alert_non_persist_id+=1
        data = {'msg': msg, 'go_to': go_to, 'time': str(datetime.utcnow()), 'viewed': False, 'id':id_}


        if redirect_msg_id:
            data['redirect_msg_id'] = redirect_msg_id
        data['redirect_resp'] = redirect_resp
        if redirect_resp:
            resp_type = None
            _color = None
            if resp_error:
                _color = 'error'
                resp_type = 'error'
            elif resp_warning:
                _color = 'alert'
                resp_type = 'warning'
            else:
                _color = 'primary'
                resp_type = 'ok'

            if not color:
                color = _color
            data['redirect_resp_type'] = resp_type

        if color:
            data['color'] = color

        if sids:
            if not slave_uuid:
                raise ValueError("session id provided without slave uuid")
            slave_uuids = []
        if slave_uuid:
            slave_uuids.append(slave_uuid)

        if slave_id:

            with self.master_db.scoped_session() as session:
                slave = session.query(masterdb.Slave).filter(masterdb.Slave.id==slave_id).first()
                if not slave:
                    raise ValueError("Unknown Slave [{}]".format(slave_id))

                if slave.active and slave.init:
                    slave_uuids.append(slave.uuid)


        if not len(slave_uuids):
            sids = []
            with self.master_db.scoped_session() as session:
                slaves = session.query(masterdb.Slave)\
                    .filter(
                        masterdb.Slave.slave_type_id==self.MASTER_DB_SLAVE_TYPE_CPV2_ID,
                        self.filter_slave_is_ready
                    ).all()

                if not len(slaves):
                    if throw_error_no_CPv2:
                        raise TypeError("Slave type 'CPv2' doesn't exist")
                    return False

                slave_uuids = [slave.uuid for slave in slaves]


        alert = data
        if persist:
            with self.master_db.scoped_session() as session:

                alert = session.scoped.add_model(masterdb.Alert, data, commit=True)
                alert = self.master_db.as_json(alert)

        else:
            for uuid in slave_uuids:
                loc = create_local_task_message(
                    'bd.sd.@CPv2.users.alerts',
                    {"alerts":[alert], "sids": sids}
                )
                self.send_message_to(uuid, loc)
        return True


    def upload_logs(self):
        try:
            tmp_path = self.log_out_path + '.tmp'
            os.rename(self.log_out_path, tmp_path) #file handler automatically creates new file

            expire_ts = str((datetime.utcnow() + timedelta(minutes=3)).timestamp())



            #data = self.gofile.upload(files=open(tmp_path,'rb'), expire=expire_ts, password=None)
            data = None



            '''
            with self.master_db.scoped_session() as session:
                st = session.query(masterdb.SlaveType).filter(masterdb.SlaveType.model_tag=='CPv2').first()
                if st:
                    slaves = session.query(masterdb.Slave).filter(masterdb.Slave.slave_type_id==st.id, masterdb.Slave.active==True).all()
            '''
            #In the future have a function that checks if you have an active user present on site
            #so it is not uploading logs when not needed
            for slave in self.get_slaves_by_model_tag('CPv2'):
                msg = create_local_task_message(
                    'bd.sd.@CPv2.logs.master',
                    data
                )
                self.send_message_to(slave.uuid, msg)
        except Exception as e:
            self.log.critical(traceback.format_exc(), path='bd.@md.log.uploads')
            raise e


    @DBWrapper.scoped_session_func(attr='master_db', expire_on_commit=False)
    def get_slave_by(self, session=None, **kwargs):
        key = list(kwargs.keys())[0]
        return session.query(masterdb.Slave).filter(getattr(masterdb.Slave, key)==kwargs[key]).first()

    @DBWrapper.scoped_session_func(attr='master_db')
    def get_slave_by_uuid(self, uuid, session=None):
        return self.get_slave_by(session=session, uuid=uuid)


    def get_slaves_by_model_tag(self, tag, active=True):
        with self.master_db.scoped_session() as session:
            st = session.query(masterdb.SlaveType).filter(masterdb.SlaveType.model_tag==tag).first()
            if st:
                return session.query(masterdb.Slave).filter(masterdb.Slave.slave_type_id==st.id, masterdb.Slave.active==active).all()
            return []


    def check_delete_records(self):
        #handle this better
        has = False
        global DELETE_RECORD_UPDATES
        for table in DELETE_RECORD_UPDATES.keys():
            if len(DELETE_RECORD_UPDATES[table]):
                has = True
                break
        if has:
            msg = create_local_task_message(
                'bd.sd.@CPv2.db.objs.delete',
                DELETE_RECORD_UPDATES
            )

            for slave in self.get_slaves_by_model_tag('CPv2'):
                self.send_message_to(slave.uuid, msg)
            DELETE_RECORD_UPDATES = {}


    def check_slave_tasks(self):
        with self.master_db.scoped_session() as session:
            tasks = session.query(masterdb.SlaveTask)\
                .filter(
                    masterdb.SlaveTask.active==True,
                    masterdb.SlaveTask.error==False,
                    masterdb.SlaveTask.assigned_slave_id.is_(None)
                ).order_by(masterdb.SlaveTask.time_created.asc()).all()

            job_map = {}
            #maybe store job_map until u know db has been updated
            waiting_job_ids = []
            needed_slave_type_ids = []

            for task in tasks:
                try:
                    job_map[task.job_id]
                except KeyError:
                    job_map[task.job_id] = []
                finally:
                    job_map[task.job_id].append(task)

                waiting_job_ids.append(task.job_id)
                needed_slave_type_ids.append(task.slave_type_id)


            jobs = session.query(masterdb.SlaveJob)\
                .filter(
                    masterdb.SlaveJob.id.in_(waiting_job_ids),
                    masterdb.SlaveJob.completed==False,
                    masterdb.SlaveJob.error==False
                ).order_by(masterdb.SlaveJob.priority.desc(), masterdb.SlaveJob.created_time).all()


            if not jobs:
                return

            CUR_MAX_JOB_PRIORITY = jobs[0].priority

            slaves = session.query(masterdb.Slave)\
                .filter(
                    masterdb.Slave.active==True,
                    #masterdb.Slave.assigned_slave_id==None,
                    #masterdb.Slave.working==False,
                    masterdb.Slave.slave_type_id.in_(needed_slave_type_ids)
                ).order_by(masterdb.Slave.id.desc()).all()


            if not slaves:
                return

            found_slave_type_ids = [slave.slave_type_id for slave in slaves]
            idle_slaves = [slave for slave in slaves if (not slave.working) and slave.is_init]

            waiting_task_ids = []
            waiting_type_task_ids = []
            needed_slave_type_ids = []
            for job in jobs:
                job_tasks = job_map[job.id]
                for task in job_tasks:
                    found_idle_slave = False
                    for i, slave in enumerate(idle_slaves):
                        if slave.slave_type_id == task.slave_type_id:
                            found_idle_slave = True
                            slave = idle_slaves.pop(i)

                            task.assigned_slave_id = slave.id
                            task.msg = 'Assigned to slave [{}]'.format(slave.id)


                            if job.stage == 'Queued':
                                job.stage = 'Running'

                            self.slave_is_working(slave=slave, working=True, task_id=task.id, session=session)

                            session.commit()
                            task_data = json.loads(task.data)
                            task_meta = json.loads(task.route_meta)

                            self.send_slave_global_task(slave.uuid, task.route, task_data, task.id, task.job_id, route_meta=task_meta)
                            self.log.info('Assigned Task [{}] to Slave [{}]'.format(task.id, slave.id), path='bd.@md.check.slave.tasks')
                            break

                    if not found_idle_slave: #either means no idle slaves or non online
                        if not task.slave_type_id in found_slave_type_ids:
                            if not task.slave_type_id in needed_slave_type_ids:
                                needed_slave_type_ids.append(task.slave_type_id)

                            waiting_type_task_ids.append(task.id)
                        else:
                            waiting_task_ids.append(task.id)

            if waiting_type_task_ids:
                self.log.error("{} task(s) are waiting for slave type(s) {} to be launched...".format(len(waiting_type_task_ids), needed_slave_type_ids))
            if waiting_task_ids:
                self.log.warning("{} task(s) are waiting for available slaves...".format(len(waiting_task_ids)), path='bd.@md.check.slave.tasks')

    def handle_bot_usage(self, usage):
        with self.master_db.scoped_session() as session:
            st = session.query(masterdb.SlaveType)\
                .filter(masterdb.SlaveType.model_tag=='CPv2')\
                .first()
            slaves = session.query(masterdb.Slave)\
                .filter(
                    masterdb.Slave.active==True,
                    masterdb.Slave.is_init==True,
                    masterdb.Slave.slave_type_id==st.id
                ).all()
            for slave in slaves:
                msg = create_local_task_message(
                    "bd.sd.@CPv2.usage.master",
                    usage
                )
                self.send_message_to(slave.uuid, msg)

    def check_slave_schedules(self):
        with self.master_db.scoped_session(expire_on_commit=False) as session:

            groups = session.query(masterdb.SchedulerGroup).all()
            for group in groups:
                schedulers = session.query(masterdb.Scheduler)\
                        .filter(
                            masterdb.Scheduler.scheduler_group_id==group.id
                        ).all()
                for scheduler in schedulers:
                    if datetime.utcnow() > scheduler.run_time and not scheduler.executed:
                        r_data = json.loads(scheduler.data)
                        r_meta = json.loads(scheduler.route_meta)
                        td = create_local_task_message(scheduler.route, r_data, route_meta=r_meta)
                        td['name'] = 'Launcher'
                        data = {
                            "job_data": {'name': group.name},
                            'tasks_data': [
                                td
                            ]
                        }


                        if scheduler.frequency_min:
                            new_run_time = datetime.utcnow() + timedelta(minutes=scheduler.frequency_min)
                            scheduler.run_time = new_run_time
                            ###################3
                            #################notify_cpv1 with new updated times
                        else:
                            scheduler.executed = True
                            #maybe delete them or do something else

                        job, tasks = self._slave_job_add(data, session=session)
                        if job:
                            msg = "Job [{}] '{}' added by Scheduler [{}] '{}'".format(job.id, job.name, group.id, group.name)
                            self.alert_CPv2(msg, go_to='/job/{}'.format(job.id), redirect_resp=True)



            session.commit()



    @DBWrapper.scoped_session_func(attr='master_db')
    def _slave_task_add(self, data, session=None):
        model_tag = None
        try:

            model_tag = self.get_model_tag_from_route(data['route'])
        except Exception as e:
            if 'job_id' in data.keys():
                self._slave_job_error(job_id=data['job_id'], msg=str(e), stage='Failed', session=session)
                #raise e

        if not model_tag in ['bd', 'md', 'sd'] and not 'slave_type_id' in data.keys():
            slave_type_id = session.query(masterdb.SlaveType.id)\
                .filter(masterdb.SlaveType.model_tag==model_tag)\
                .first()

            if slave_type_id:
                slave_type_id = slave_type_id[0]
                data['slave_type_id'] = slave_type_id

        task = None
        if not 'job_id' in data.keys():
            job_data = {"job_data":{"name": "Anon Job", 'hidden': True}, "tasks_data": [data]}
            job, tasks = self._slave_job_add(job_data, session=session)
            task = tasks[0]

        else:
            task = session.scoped.add_model(masterdb.SlaveTask, data)
            if task.error:
                self._slave_task_error(task=task, msg=task.msg, time_ended=datetime.utcnow(), session=session)

            session.commit()

        return task

    @DBWrapper.scoped_session_func(attr='master_db')
    def job_has_active_tasks(self, job_id, session=None):
        more_tasks = session.query(masterdb.SlaveTask)\
            .filter(
                and_(
                    masterdb.SlaveTask.job_id==job_id,
                    masterdb.SlaveTask.completed==False,
                    masterdb.SlaveTask.error==False
                )
            ).first()

        return more_tasks != None
    @DBWrapper.scoped_session_func(attr='master_db', expire_on_commit=False)
    def _slave_task_completed(self, *, task_id=None, task=None, time_ended, session=None):
        with self.master_db.scoped_session() as session:
            if task_id:
                task = session.query(masterdb.SlaveTask)\
                    .filter(masterdb.SlaveTask.id==task_id)\
                    .first()

            if not task:
                raise ValueError("Task [{}] doesn't exist".format(task_id))
            task.completed = True
            task.time_ended = time_ended
            if not self.get_bot_config("persistent_task_data"):
                task.data = None
                task.route_meta = None

            self.slave_is_working(slave_id=task.assigned_slave_id, working=False, session=session)

            session.commit()

            if not self.job_has_active_tasks(task.job_id, session=session):
                msg = self.create_local_task_message(
                    "bd.@md.slave.job.completed",
                    {"job_id":task.job_id}
                )
                self.add_local_msg(msg, INBOX_SYS_MSG, delay=3)


    @DBWrapper.scoped_session_func(attr='master_db')
    def _slave_job_completed(self, *, job=None, job_id=None, msg=None, session=None):
        if job_id:

            job = session.query(masterdb.SlaveJob).with_for_update()\
                .filter(masterdb.SlaveJob.id==job_id)\
                .first()

        if not job:
            raise ValueError("Job [{}] doesn't exist".format(job_id))

        if job.error:
            return False

        if self.job_has_active_tasks(job_id, session=session):
            return False

        if not msg:
            msg = 'OK'

        job.completed = True
        job.stage = 'Done'
        job.msg = msg
        return True

    @DBWrapper.scoped_session_func(attr='master_db', expire_on_commit=False)
    def _slave_job_add(self, data, session=None):

        job = session.scoped.add_model(masterdb.SlaveJob, data['job_data'], commit=True)
        tasks = []
        if 'tasks_data' in data.keys():
            for task in data['tasks_data']:
                task['job_id'] = job.id
                task_obj = self._slave_task_add(task, session=session)
                tasks.append(task_obj)

        return job, tasks


    @DBWrapper.scoped_session_func(attr='master_db', expire_on_commit=False)
    def _slave_task_error(self, *, task_id=None, task=None, msg='Task had an unexpected error', time_ended=None, check_job=True, other_task_failed=False, session=None):
        if task_id:
            task = session.query(masterdb.SlaveTask)\
            .filter(masterdb.SlaveTask.id==task_id)\
            .first()

        if not task:
            raise ValueError("Task [{}] doesn't exist".format(task_id))

        if task.error:
            return False

        if task.assigned_slave_id and not ( task.completed or task.error):
            self.slave_is_working(slave_id=task.assigned_slave_id, working=False, force_stop=True, session=session)

        if other_task_failed:
            if task.completed:
                msg = 'Was OK'
            elif task.started:
                msg = 'Was In-Progress'
            else:
                msg = 'Was Queued'

        task.error = True
        task.msg = msg
        task.time_ended = time_ended

        session.commit()
        if task.job_ok_on_error:
            if not self.job_has_active_tasks(task.job_id, session=session):
                msg = self.create_local_task_message(
                    "bd.@md.slave.job.completed",
                    {"job_id":task.job_id}
                )
                try:
                    self.add_local_msg(msg, INBOX_SYS_MSG, delay=3)
                except:
                    #this is running before inbox is initialized
                    pass
            return True

        RETRY_OK = True
        if task.job_id:
            job = session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==task.job_id)\
                .first()

            RETRY_OK = not job.error

        if RETRY_OK:
            if task.retry_cnt >=1:
                task_retry_payload = {}
                max_retry = 0
                name = task.name
                if task.retried_task_id:
                    task_retry_payload['retried_task_id']  = task.retried_task_id
                    parent_task = session.query(masterdb.SlaveTask)\
                        .filter(masterdb.SlaveTask.id==task.retried_task_id)\
                        .first()

                    name = parent_task.name
                    max_retry = parent_task.retry_cnt
                else:
                    task_retry_payload['retried_task_id'] = task.id
                    max_retry = task.retry_cnt

                cnt = task.retry_cnt - 1
                display_cnt = max_retry-cnt
                task_retry_payload['retry_cnt'] = cnt
                task_retry_payload['job_id'] = task.job_id
                task_retry_payload['slave_type_id'] = task.slave_type_id
                task_retry_payload['job_ok_on_error'] = task.retry_cnt>0
                task_retry_payload['data'] = task.data
                task_retry_payload['route'] = task.route
                task_retry_payload['name'] = "{} (Retry {}/{})".format(name, display_cnt, max_retry)
                retrying_task = session.scoped.add_model(masterdb.SlaveTask, task_retry_payload)

                session.commit()
                #retries left so finished func
                return True

            elif task.retry_cnt<=0:
                #no retries left
                check_job = True
                task.job_ok_on_error = False
                pass

        if task.job_id and check_job:
            if not task.job_ok_on_error:
                err_msg = "Job was stopped because task [{}] failed max tries".format(task.id)
                stage_msg = "Failed Retries"
                if task.retry_cnt == -1:
                    err_msg = 'task [{}] failed: "{}"'.format(task.id, msg)
                    stage_msg = "Failed"
                self._slave_job_error(job_id=task.job_id, msg=err_msg, stage=stage_msg, session=session)


            elif task.task_group_id:
                #handle task_group
                pass
        session.commit()
        return True

    @DBWrapper.scoped_session_func(attr='master_db')
    def _slave_job_error(self, *, job_id=None, job=None, msg='Job had an unexpected error', task_msg=None, stage='Failed', session=None):
        if not task_msg:
            task_msg = msg

        if job_id:
            job = session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==job_id)\
                .first()

        if not job:
            raise ValueError("Job [{}] doesn't exist".format(job_id))

        if job:
            job.error = True
            job.completed = False
            job.msg = msg
            job.stage = stage
            for job_task in job.tasks:
                if not job_task.error:
                    self._slave_task_error(task_id=job_task.id, msg=task_msg, check_job=False, time_ended=datetime.utcnow(), other_task_failed=True,session=session)


    def check_slave_recent_resp_times(self):
        inactive_slaves = []
        with self.master_db.scoped_session() as session:
            slaves = session.query(masterdb.Slave).filter(
                masterdb.Slave.active==True,
                or_(
                    masterdb.Slave.last_pulse+timedelta(seconds=60) <= datetime.utcnow(),
                    masterdb.Slave.is_init==False
                    )
            ).all()

            now = datetime.utcnow()
            for slave in slaves:
                inactive = False

                if not slave.is_init:
                    if (slave.first_pulse + timedelta(seconds=self.get_bot_config("slave_init_grace") )) <= now:
                        inactive = True

                elif (slave.last_pulse + timedelta(seconds=60)) <= now:
                    inactive = True


                if inactive:
                    inactive_slaves.append(slave)



        if inactive_slaves:
            self.log.warning("Slave(s) {} have been unresponsive -- Terminating...".format([slave.id for slave in slaves]), path='bd.@md.slave.recent.check')
            self.remove_slaves(slaves=inactive_slaves, err_msg=' was unresponsive', session=session)

        uuids = self.slaves_recent_resp_times.keys()
        for uuid in uuids:
            if len(self.slaves_recent_resp_times[uuid])>10:
                self.slaves_recent_resp_times[uuid] = []



    def bd_md_tag_send(self, data, route_meta):
        tag_data = data.get('data', None)
        tag_datas = data.get('datas', [])
        if tag_data:
            tag_datas.append(tag_data)

        msg = create_local_task_message("@bd.tag.add", {"tag":data["tag"], "datas": tag_datas}, route_meta=route_meta)
        self.send_message_to(self.get_tag_uuid(data['tag']), msg, from_uuid=route_meta['origin'])

    def bd_md_slave_launch(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            if not 'slave_type_id' in data.keys():
                st = session.query(masterdb.SlaveType)\
                    .filter(masterdb.SlaveType.model_tag==data['model_tag'])\
                    .first()
                data['slave_type_id'] = st.id


            if data.get('is_ec2', False):
                cnt = data.get('count', 1)
                self.log.info("Trying to launch {} ec2 slave(s)".format(cnt))
                """
                TMP solution to launch multiple ec2 instances at once
                Will change to using cpv2 as a middle man for initilization
                """
                #for i in range(0, cnt):
                #    self.launch_ec2_instance(data['slave_type_id'], session=session)
                self.launch_ec2_instances(data['slave_type_id'], cnt, session=session)
            else:
                raise ValueError("Unknown slave host type")


    def terminate_ec2_instances(self, *instance_ids):
        self.log.debug("Terminating ec2 instances {}".format(instance_ids))
        self.ec2_resource.instances.filter(InstanceIds=instance_ids).terminate()

    @property
    def filter_slave_is_ready(self):
        return and_(
            masterdb.Slave.active==True,
            masterdb.Slave.is_init==True
        )


    def launch_ec2_instance(self, slave_type_id, cfgs_url=None, slave_url=None, session=None):
        self.allowed_to_launch_ec2_instances()
        slave_url, cfgs_url = self.check_slave_type_download_urls(slave_type_id, slave_url, cfgs_url, session=session)

        error = None
        slave_uuid = self.create_bot_uuid()
        slave_info = {
            "is_ec2": True,
            "uuid": slave_uuid,
            "ec2_instance_id": None,
            "active": True,
            "is_init": False,
            "slave_type_id": slave_type_id
        }

        slave = session.scoped.add_model(masterdb.Slave, slave_info, commit=True)
        slave_token = self.create_route_token(slave_uuid, *SLAVE_LEVEL_GROUP, slave=slave)

        added_cfgs = [
            {"key": "master_ip", "value": self.get_bot_config("master_ip")},
            {"key": "master_port", "value": self.get_bot_config("master_port")},
            {"key": "uuid", "value": slave_uuid},
            {"key": "token", "value": slave_token}
        ]

        ec2_slave = None
        try:
            ec2_slave = self.ec2_resource.create_instances(
                ImageId=self.get_bot_config("aws_ec2_image_id_slave"), #ubuntu 18
                UserData=self.create_ec2_start_script(
                    extra_configs=added_cfgs,
                    configs_download_url=cfgs_url,
                    slave_download_url=slave_url
                ),
                MinCount=1, MaxCount=1,
                InstanceType='t2.micro',
                SecurityGroupIds=[self.get_bot_config("aws_ec2_security_group_slave")],
                KeyName=self.get_bot_config("aws_ec2_key_pair_slave"),
                TagSpecifications=[{'ResourceType': 'instance', 'Tags':[{'Key':'launch-type', 'Value': 'master'}]}]
            )[0]
        except Exception as e:
            error = e
            #self.set_bot_config("use_slave_host_ec2", False)
            self.log.error(e)
            err_msg = str(e)
            slave.active = False

        if ec2_slave:
            #slave_info['active'] = True
            #slave_info['last_pulse'] = datetime.utcnow()+timedelta(seconds=self.get_bot_config("slave_init_grace"))
            #slave_info['ec2_instance_id'] = ec2_slave.instance_id
            slave.ip = ec2_slave.public_ip_address
            slave.last_pulse = datetime.utcnow()+timedelta(seconds=self.get_bot_config("slave_init_grace"))
            slave.ec2_instance_id = ec2_slave.instance_id
        session.commit()


        if not ec2_slave:
            raise error


        self.ec2_resource.create_tags(
            Resources=[ec2_slave.instance_id],
            Tags=[
                {'Key': 'slave.id', 'Value': str(slave.id)},
                {'Key': 'bot.type', 'Value': 'slave'},
                {'Key': 'slave.type.id', 'Value': str(slave.slave_type_id)},
                {'Key': 'Name', 'Value': 'sd-'+str(slave.id)}
            ]
        )


    def allowed_to_launch_ec2_instances(self):
        if not self.get_bot_config("bn2_lib_download_url"):
            raise ValueError("bn2_lib_download_url needs to be provided")

        if not self.get_bot_config('use_slave_host_ec2'):
            raise ValueError("use_slave_host_ec2 bot config is disabled")

        if not hasattr(self, 'ec2_resource'):
            raise ValueError("EC2 Resource wasn't initialized correctly -- Restart master")

        return True
    def create_ec2_start_script_multiple(self, configs_download_url=None, slave_download_url=None, creds_download_url=None, extra_configs=[]):
        has_extra_configs = 'false'
        if len(extra_configs) > 0:
            extra_configs = ",".join(json.dumps(c) for c in extra_configs)
            has_extra_configs = 'true'

        APPEND_JSON_CMD = """
        qt='"' #dirty trick for double quotes... ahh bash :)
        echo -n ",{${qt}key${qt}:${qt}token${qt},${qt}value${qt}:${qt}$token${qt}},{${qt}key${qt}:${qt}uuid${qt},${qt}value${qt}:${qt}$uuid${qt}}]" >> configs.json
        """

        return """#!/bin/bash
        extract () {{
        #extracts files based on if zip/tar and strip root folder
            if (file $1 | grep -q "tar archive" ) ;

            then
                tar -xvf $1 --one-top-level=$2 --strip-components 1
                echo "is tar"
            elif (file $1 | grep -q "Zip archive") ;
            then
                echo "is zip"
                unzip $1 -d _tmp_dir
                mkdir $2
                cp -r _tmp_dir/*/* $2/.
                rm -r _tmp_dir
            fi
        }}

        cd  /home/ubuntu
        sudo apt-get update
        sudo apt-get install python3-pip unzip -y

        wget {cfgs_url} -O configs.json
        wget {creds_url} -O creds.txt

        #this allows us to append to json config file correctly
        if     [ -n "$(tail -c1 configs.json)" ]
        then
            truncate -s-1 configs.json
        else
            truncate -s-2 configs.json
        fi

        if {has_extra_cfgs}
        then
            echo -n ',{extra_cfgs}' >> configs.json
        fi

        uuid=$(cat creds.txt | sed -n "1 p")
        token=$(cat creds.txt | sed -n "2 p")

        {json_cmd}

        #download slave lib and extract here
        wget {slave_url} -O slave.archive
        extract slave.archive slave

        #download bn2 lib and extract here
        wget {bn2_lib_url} -O bn2.archive
        extract bn2.archive bn2

        ln -s -f /home/ubuntu/bn2 /home/ubuntu/slave/.

        sudo pip3 install -r slave/requirements.txt
        bash slave/setup.sh

        ## this will install pip and apt-get items needed

        python3 slave/slave.py configs.json &

        """.format(
            cfgs_url=configs_download_url,
            has_extra_cfgs=has_extra_configs,
            extra_cfgs=extra_configs,
            slave_url=slave_download_url,
            creds_url=creds_download_url,
            bn2_lib_url=self.get_bot_config("bn2_lib_download_url"),
            json_cmd=APPEND_JSON_CMD
        )


    def create_ec2_start_script(self, configs_download_url=None, slave_download_url=None, extra_configs=[]):
        has_extra_configs = 'false'
        if len(extra_configs) > 0:
            extra_configs = ",".join(json.dumps(c) for c in extra_configs)
            has_extra_configs = 'true'

        return """#!/bin/bash
        extract () {{
        #extracts files based on if zip/tar and strip root folder
            if (file $1 | grep -q "tar archive" ) ;

            then
                tar -xvf $1 --one-top-level=$2 --strip-components 1
                echo "is tar"
            elif (file $1 | grep -q "Zip archive") ;
            then
                echo "is zip"
                unzip $1 -d _tmp_dir
                mkdir $2
                cp -r _tmp_dir/*/* $2/.
                rm -r _tmp_dir
            fi
        }}

        cd  /home/ubuntu
        sudo apt-get update
        sudo apt-get install python3-pip unzip -y

        wget {cfgs_url} -O configs.json

        if {has_extra_cfgs}
        then
            #this allows us to append to json config file correctly
            if     [ -n "$(tail -c1 configs.json)" ]
            then
                truncate -s-1 configs.json
            else
                truncate -s-2 configs.json
            fi
            echo -n ',{extra_cfgs}]' >> configs.json
        fi

        #download slave lib and extract here
        wget {slave_url} -O slave.archive
        extract slave.archive slave

        #download bn2 lib and extract here
        wget {bn2_lib_url} -O bn2.archive
        extract bn2.archive bn2

        ln -s -f /home/ubuntu/bn2 /home/ubuntu/slave/.

        sudo pip3 install -r slave/requirements.txt
        bash slave/setup.sh

        ## this will install pip and apt-get items needed

        python3 slave/slave.py configs.json &

        """.format(
            cfgs_url=configs_download_url,
            has_extra_cfgs=has_extra_configs,
            extra_cfgs=extra_configs,
            slave_url=slave_download_url,
            bn2_lib_url=self.get_bot_config("bn2_lib_download_url")
        )


    @DBWrapper.scoped_session_func(attr='master_db')
    def check_slave_type_download_urls(self, slave_type_id, slave_url, cfgs_url, *, session=None):
        if not cfgs_url or slave_url:
            download_urls = session.query(masterdb.SlaveType.cfgs_url, masterdb.SlaveType.slave_url)\
                .filter(masterdb.SlaveType.id==slave_type_id)\
                .first()
            if not (download_urls[0] and download_urls[1]):
                raise ValueError("Slave type [{}] doesn't have download urls".format(slave_type_id))

            if not cfgs_url:
                cfgs_url = download_urls[0]
            if not slave_url:
                slave_url = download_urls[1]

        if not slave_url:
            raise ValueError("Slave type [{}] doesn't have slave download url".format(slave_type_id))
        return slave_url, cfgs_url


    @DBWrapper.scoped_session_func(attr='master_db')
    def launch_ec2_instances(self, slave_type_id, cnt=1, *, cfgs_url=None, slave_url=None, session=None):
        error = None
        self.allowed_to_launch_ec2_instances()

        slave_url, cfgs_url = self.check_slave_type_download_urls(slave_type_id, slave_url, cfgs_url, session=session)
        cpv2_slave  = session.query(masterdb.Slave).filter(masterdb.Slave.slave_type_id==self.MASTER_DB_SLAVE_TYPE_CPV2_ID, self.filter_slave_is_ready).first()
        slaves = []
        if not cpv2_slave:
            raise ValueError("CPv2 slave type needs to be online to launch more than 5 instances")


        added_cfgs = [
            {"key": "master_ip", "value": self.get_bot_config("master_ip")},
            {"key": "master_port", "value": self.get_bot_config("master_port")}
        ]

        launch_tag = self.create_tag(cpv2_slave.uuid)
        creds_url = urllib.parse.quote("{}/api/slaves/launched/{}".format(
            cpv2_slave.ip,
            launch_tag
        ))

        for i in range(0, cnt):
            slave_info = {
                "is_ec2": True,
                "uuid": self.create_bot_uuid(),
                "ec2_instance_id": None,
                "active": True,
                "is_init": False,
                "slave_type_id": slave_type_id
            }
            slaves.append(session.scoped.add_model(masterdb.Slave, slave_info))
        session.commit()


        ec2_slaves = []
        try:
            ec2_slaves = self.ec2_resource.create_instances(
                ImageId=self.get_bot_config("aws_ec2_image_id_slave"), #ubuntu 18
                UserData=self.create_ec2_start_script_multiple(
                    extra_configs=added_cfgs,
                    configs_download_url=cfgs_url,
                    slave_download_url=slave_url,
                    creds_download_url=creds_url
                ),
                MinCount=1, MaxCount=cnt,
                InstanceType='t2.micro',
                SecurityGroupIds=[self.get_bot_config("aws_ec2_security_group_slave")],
                KeyName=self.get_bot_config("aws_ec2_key_pair_slave"),
                TagSpecifications=[{'ResourceType': 'instance', 'Tags':[{'Key':'launch_tag', 'Value': launch_tag}]}]
            )
        except Exception as e:
            error = e
            err_msg = str(e)

        got_cnt = len(ec2_slaves)
        self.log.debug("Got {} AWS EC2 instances".format(got_cnt))
        if got_cnt != cnt:
            self.log.warning("AWS EC2 only launched {} instances when requested {}".format(got_cnt, cnt))
            for j in range(0, cnt-got_cnt):
                slave = slaves.pop(-1)
                slave.active = False
            session.commit()

        if ec2_slaves:


            launcher_group = []
            for i in range(0, got_cnt):
                slave = slaves[i]
                ec2_slave = ec2_slaves[i]
                slave.ip = ec2_slave.public_ip_address
                slave.last_pulse = datetime.utcnow()+timedelta(seconds=self.get_bot_config("slave_init_grace"))
                slave.ec2_instance_id = ec2_slave.instance_id

                slave_token = self.create_route_token(slave.uuid, *SLAVE_LEVEL_GROUP, slave=slave)
                launcher_group.append({"token":slave_token, "uuid":slave.uuid})

            session.commit()
            self.log.info("SENDING TO LANUCH TAG: {}".format(launch_tag))
            self.send_tag_data(launch_tag, *launcher_group)

        if error:
            raise error

    def bd_md_slave_kill(self, data, route_meta):
        uuid = data.get('uuid', None)
        sid = slave_id=data.get('id', None)
        uuids = data.get('uuids', [])
        sids = data.get('ids', [])
        for uuid in uuids:
            self.kill_slave(uuid=uuid)
        for sid in sids:
            self.kill_slave(slave_id=sid)

    def kill_slave(self, *, err_msg=None, uuid=None, slave_id=None):
        #in future will check if aws and will terminate
        if slave_id:
            slave = self.get_slave_by(id=slave_id)
            if slave:
                uuid = slave.uuid

        if uuid==None:
            raise NotImplemented("slave [{}] doesn't exist".format(slave_id))


        if not err_msg:
            err_msg = 'Killed by master'

        msg = self.create_local_task_message(
            "@bd.die",
            {"msg": err_msg}
        )

        self.send_message_to(uuid, msg)

        #self.send_comm_cmd("del_connection", target_uuid=uuid)

    def bd_md_network_destruct(self, data, route_meta, token):
        username = token.get('username', None)
        if not username:
            username = "MASTER"
        with self.master_db.scoped_session() as session:
            jobs = session.query(masterdb.Slave).filter(masterdb.SlaveJob.completed==False, masterdb.SlaveJob.error==False).all()
            msg = 'Job was stopped due to network self-destruct by user [{}]'.format(username)
            for job in jobs:
                self._slave_job_error(job_id=job.id, msg=msg, session=session)
            session.commit()


            slaves = session.query(masterdb.Slave).filter(masterdb.Slave.active==True).all()
            self.remove_slaves(slaves=slaves, session=session)
            session.commit()
        self.die("Self-Destructed thanks to {}".format(username))

    def bd_md_slave_pulse(self, data, route_meta, token):
        dt = datetime.utcnow()
        task_id = None

        SEND_STOP_TASK = False
        with self.master_db.scoped_session() as session:
            slave = session.query(masterdb.Slave).filter(masterdb.Slave.id==token['slave_id']).first()
            if not slave:
                self.log.error("Slave [{}] was not correctly initialized or from a past master instance -- Terminating".format(token['slave_id']))
                self.kill_slave(uuid=token['origin'])
                return

            running_slave_task = data.get('running_slave_task', False)
            if (slave.assigned_task_id and not running_slave_task):
                task = session.query(masterdb.SlaveTask).filter(masterdb.SlaveTask.id==slave.assigned_task_id).first()

                SEND_STOP_TASK = True
                if task:
                    if task.error or task.completed:
                        self.log.warning("DB did not release Slave [{}] on task error/completion -- doing it now.".format(slave.id))
                        self.slave_is_working(slave, working=False, session=session)
                        SEND_STOP_TASK = False
                    else:
                        self.log.warning("DB reports Slave [{}] is assigned to {} but slave returns working={} -- could be that msg is yet to be processed".format(slave.id, slave.assigned_task_id, running_slave_task))


            elif (not slave.working) and slave.is_ec2:
                if (slave.first_pulse + timedelta(seconds=self.get_bot_config("slave_idle_grace"))) <= datetime.utcnow():
                    #check how recent the last task it worked on was and terminate over certain period
                    flag = session.query(masterdb.SlaveTask).filter(
                        masterdb.SlaveTask.time_ended>datetime.utcnow()- timedelta(seconds=self.get_bot_config("slave_idle_grace"))
                    ).first()

                    if not flag:
                        self.log.debug("Terminating Slave [{}] -- hasn't worked in {} seconds".format(slave.id, self.get_bot_config("slave_idle_grace")))
                        self.kill_slave(uuid=slave.uuid)
                        #self.remove_slaves(slave=slave, send_comm_cmd=False, session=session)
                        return

            slave.last_pulse = dt
            slave.active = True
            task_id = slave.assigned_task_id
            session.commit()



        r = (dt - datetime.strptime(data['send_time'], "%Y-%m-%d %H:%M:%S.%f")).total_seconds() * 1000
        r = int(r)
        slave_resps = self.slaves_recent_resp_times.get(route_meta['origin'], [])
        slave_resps.append(r)
        self.slaves_recent_resp_times[route_meta['origin']] = slave_resps
        self.log.debug("slave [{}] pulse -- {}ms latency".format(token['slave_id'], r))
        if SEND_STOP_TASK and task_id:
            #implement delayed local_msg
            msg = self.create_local_task_message(
                "bd.@md.slave.task.check",
                {"task_id": task_id, "is_started": True, "msg": "Task [{}] was corrupted".format(task_id)}
            )
            self.add_local_msg(msg, INBOX_TASK1_MSG, delay=10)

    def bd_md_slave_tasks_check(self, data, route_meta):
        self.check_slave_tasks()

    def bd_md_slave_pulses_check(self, data, route_meta):
        self.check_slave_recent_resp_times()

    @DBWrapper.scoped_session_func(attr='master_db', expire_on_commit=False)
    def remove_slaves(self, _id=None, ids=[], slave=None, slaves=[], uuid=None, uuids=[], err_msg=None, send_comm_cmd=True, send_kill_slave=True, session=None):
        if not err_msg:
            err_msg = "was lost unexpectedly"

        if slaves:
            pass
        elif slave:
            slaves.append(slave)
        elif uuids or ids:
            query = session.query(masterdb.Slave)
            if uuids:
                query = query.filter(masterdb.Slave.uuid.in_(uuids))
            elif ids:
                query = query.filter(masterdb.Slave.id.in_(ids))
            slaves = query.all()

        else:
            slave_q = session.query(masterdb.Slave).filter(or_(masterdb.Slave.uuid==uuid, masterdb.Slave.id==_id))
            """
            if _id:
                slave_q = slave_q.filter(masterdb.Slave.id==_id)
            else:
                slave_q = slave_q.filter(masterdb.Slave.uuid==uuid)
            """
            slaves = slave_q.all()


        ec2_ids = []
        for slave in slaves:
            if slave.uuid in self.slaves_recent_resp_times.keys():
                del self.slaves_recent_resp_times[slave.uuid]

            slave.active = False

            if slave.working and slave.assigned_task_id:
                msg = "Slave [{}] {}".format(slave.id, err_msg)
                self._slave_task_error(task_id=slave.assigned_task_id, msg=msg, time_ended=datetime.utcnow(), session=session)

            if send_kill_slave:
                self.kill_slave(uuid=slave.uuid)

            if send_comm_cmd:
                self.send_comm_cmd("del_connection", target_uuid=slave.uuid)

            if slave.is_ec2:
                if slave.ec2_instance_id:
                    ec2_ids.append(slave.ec2_instance_id)
        if ec2_ids:
            try:
                self.terminate_ec2_instances(*ec2_ids)
            except Exception as e:
                self.log.error(e, path='def remove_slaves')

        session.commit()

    def bd_md_slave_lost(self, data, route_meta, token):
        #FIX THIS
        _id = None
        iden = None
        uuid = None

        if token.get('slave_id', None):
            _id = token['slave_id']
            iden = _id

        else:
            _id = data.get('id', None)
            uuid = data.get('uuid', None)
            if _id:
                iden = _id
            elif uuid:
                iden = uuid


            else:
                raise ValueError("Slave id or uuid is required argument")

        msg = data.get('err_msg', 'was lost unexpectedly')

        self.log.warning("Slave [{}] {}".format(iden, msg))

        send_ = not data.get('from_comms', False) #wont send cmds if from comms

        self.remove_slaves(_id=_id, uuid=uuid, err_msg=msg, send_comm_cmd=send_, send_kill_slave=send_)


    def bd_md_user_add(self, data, route_meta):
        success = False
        with self.master_db.scoped_session() as session:
            data['levels'].append('USER')
            data['bitmask'] = self.create_route_level_bitmask(*data['levels'])
            data['levels'] = json.dumps(data['levels'])
            user = session.scoped.add_model(masterdb.User, data, commit=True)

            success = user != None
        self.log.info("Adding user '{}' Success: {}".format(data['username'], success))
        return {"success": success}

    def bd_md_user_remove(self, data, route_meta):
        pass

    def bd_md_user_verify(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            username = data['username']
            password = data['password']
            user = session.query(masterdb.User).filter(masterdb.User.username==username).first()
            if not user:
                raise ValueError("User '{}' does not exist".format(username))
            authenticated = user.verify(password)
            token = {}

            if authenticated:
                token = self.create_route_token(route_meta['origin'], None, bitmask=user.bitmask, user_id=user.id)
                self.log.info("User '{}' was verified, token:\n{}".format(username, token))
            else:
                self.log.warning("User '{}' was not verified".format(username))

            cr_tag = data.get("tag")
            if cr_tag:
                self.send_tag_data(cr_tag, token)

            return token

    def bd_md_slave_connect(self, data, route_meta):
        with self.master_db.scoped_session() as session:

            slave = session.query(masterdb.Slave).filter(masterdb.Slave.uuid==route_meta['origin']).first()
            msg = None
            if not slave:
                self.log.info('Slave [{}] is requesting to connect from {} -- Will need to be authenticated'.format(route_meta['origin'], data['ip']))

                msg = self.create_local_task_message (
                    'bd.@sd.auth',
                    {
                        'master_uuid': self.uuid,
                        'bn2_public_key': self.bn2_public_key.decode(),
                        'verify': False
                    }
                )

            else:
                if slave.ip and not (slave.ip == data['ip']): #check if past ip is equal to same ip -- can help prevent againsts token hijacking
                    self.log.warning("Incomming connection blocked from Slave [{}] -- Accessing from new IP {}".format(slave.id, data['ip']))
                    self.send_comm_cmd("del_connection", target_uuid=route_meta['origin'])
                    return

                self.log.debug('Incomming connection from Slave [{}] -- Requesting verification'.format(slave.id))

                msg = self.create_local_task_message(
                    "bd.@sd.auth",
                    {'verify': True}
                )

            self.send_message_to(route_meta['origin'], msg, priority=OUTBOX_SYS_MSG)

    def bd_md_slave_verify(self, data, route_meta, token):
        if route_meta['origin'] != token['slave_uuid']:
            self.log.warning("Origin [{}] had token for Slave [{}] but uuid didn't match -- Disconnecting".format(route_meta['origin'], token['slave_id']))
            self.kill_slave(uuid=route_meta['origin'])
            return

        if not self.has_route_level(token['bitmask'], SLAVE_ROUTE_LEVEL) and token['slave_id']:
            self.log.warning("Slave [{}] doesn't have SLAVE route level due to token issue -- Terminating".format(token['slave_id'], route_meta['origin']))
            self.remove_slaves(_id=token['slave_id'])
            return

        if not self.has_route_level(token['bitmask'], SLAVE_ROUTE_LEVEL):
            self.log.warning("Origin [{}] is trying to verify without SLAVE route level -- Disconnecting".format(route_meta['origin']))
            self.send_comm_cmd("del_connection", target_uuid=route_meta['origin'])
            return

        self.log.debug("Verified Slave [{}] -- Marking as active".format(token['slave_id']))


        with self.master_db.scoped_session() as session:
            slave = session.query(masterdb.Slave).filter(masterdb.Slave.id==token['slave_id']).first()
            if not slave:
                raise ValueError("Slave [{}] does not exist".format(token['slave_id']))
                return

            if not slave.is_init:
                now = datetime.utcnow()
                slave.first_pulse = now
                slave.last_pulse = now
                slave.is_init = True
                slave.ip = data['ip']

                #may remove if to slow
                self.ec2_resource.create_tags(
                    Resources=[slave.ec2_instance_id],
                    Tags=[
                        {'Key': 'slave.id', 'Value': str(slave.id)},
                        {'Key': 'bot.type', 'Value': 'slave'},
                        {'Key': 'slave.type.id', 'Value': str(slave.slave_type_id)},
                        {'Key': 'Name', 'Value': 'sd-'+str(slave.id)}
                    ]
                )


            slave.active = True
            session.commit()


    def bd_md_slave_register(self, data, route_meta):
        self.log.debug('Slave {} sent registering information {}'.format(route_meta['origin'], data))
        with self.master_db.scoped_session() as session:

            slave_type = session.query(masterdb.SlaveType)\
                .filter(masterdb.SlaveType.model_tag==data['model_tag'])\
                .first()
            if slave_type:
                data['slave_type_id'] = slave_type.id

            else:
                err_msg = "Model tag '{}' is not in Slave Type Table".format(data['model_tag'])
                die = create_local_task_message(
                    '@bd.die',
                    {'msg':err_msg}
                )
                self.send_message_to(route_meta['origin'], die)

                raise ValueError(err_msg)


            data['is_ec2'] = False
            data['first_pulse'] = datetime.utcnow()
            data['last_pulse'] = datetime.utcnow() + timedelta(seconds=15)
            slave = None
            error = None
            try:
                slave = session.scoped.add_model(masterdb.Slave, data, commit=True)
            except Exception as e:
                error = e

            if slave:
                self.log.info("Registered Slave [{}] type '{}'".format(slave.id, slave_type.name))

                level_group = SLAVE_LEVEL_GROUP
                if data['model_tag'] == 'CPv2':
                    level_group = CP_SLAVE_LEVEL_GROUP

                token = self.create_route_token(route_meta['origin'], *level_group, slave=slave)

                out = create_local_task_message("bd.@sd.token", {'token':token})
                self.send_message_to(route_meta['origin'], out)

                dout1 = {'route_levels': self.route_levels}
                out1 = create_local_task_message(
                    'bd.@sd.route.levels.set',
                    dout1
                )

                self.send_message_to(route_meta['origin'], out1)


            else:
                self.kill_slave(uuid=route_meta['origin']) #it automatically kills for now to prevent slave from retrying infin.
                #maybe implement cooldown feature in future?

                #self.send_comm_cmd("del_connection", target_uuid=route_meta['origin']) uneeded
                raise error

    def bd_md_slave_message_send(self, data, route_meta):
        self.send_message_to(data['uuid'], data['msg'], from_uuid=route_meta['origin'])

    def bd_md_slave_job_add(self, data, route_meta):
        self._slave_job_add(data)

    def bd_md_slave_job_remove(self, data, route_meta):
        pass

    def bd_md_slave_job_edit(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            job = session.query(masterdb.SlaveJob).with_for_update()\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()

            data.pop('job_id')
            for k in data.keys():
                if  k in ['stage', 'send_alert', 'msg', 'name']: #allowed columns
                    setattr(job, k, data[k])
            session.commit()

    def bd_md_slave_job_error(self, data, route_meta):
        msg = data.get('msg')
        self._slave_job_error(data['job_id'], msg)

    def bd_md_slave_job_report_create(self, data, route_meta):
        with self.master_db.scoped_session() as session:


            job = session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id']).first()

            if not job:
                raise IndexError("Job [{}] does not exist".format(data['job_id']))

            if job.job_report_id:
                raise ValueError("Job [{}] already has job report [{}]".format(job.id, job.job_report_id))

            jr = session.scoped.add_model(
                masterdb.SlaveJobReport, data, commit=True
            )

            job.job_report_id = jr.id
            session.commit()
            self.log.debug("Job [{}] has created Report [{}]".format(job.id, jr.id))

    @DBWrapper.scoped_session_func(attr='master_db')
    def get_report_by_job_id(self, job_id, lock=False, session=None):

        q = session.query(masterdb.SlaveJobReport)
        if lock:
            q = q.with_for_update()
        return q.filter(masterdb.SlaveJobReport.job_id==job_id).first()

    def bd_md_slave_job_report_generate(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            job_report = self.get_report_by_job_id(data['job_id'], lock=True, session=session)
            pull = False
            if job_report:
                if len(job_report.data)==2:
                    pull = True

            if not job_report:
                pull = True

            if pull:
                self.log.debug("Job [{}] does not have report or missing info, waiting a couple seconds".format(data['job_id']))
                self.sleep(2000)
                if job_report:
                    session.refresh(job_report)
                    if len(job_report.data)>2:
                        pull = False


            if pull:
                raise IndexError("Job [{}] does not have a report or report is missing info".format(data['job_id']))

            content = None

            try:
                session.refresh(job_report)

                content = job_report.data
                if job_report.skeleton:
                    report_data = json.loads(job_report.data)
                    content = job_report.skeleton.format(**report_data)
            except KeyError as e:
                raise KeyError('Job [{}]\'s report does not have key "{}"'.format(data['job_id'], e.args[0]))

            job_report.content = content
            session.commit()
            self.log.debug("Job [{}] has generated Report [{}]: {} chars".format(data['job_id'], job_report.id, len(job_report.content)))

    def bd_md_slave_job_report_keys_set(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            job_report = None
            for i in range(0, 3):
                job_report = session.query(masterdb.SlaveJobReport).with_for_update()\
                    .filter(masterdb.SlaveJobReport.job_id==data['job_id']).first()

                if not job_report:
                    self.sleep(1000)

            if not job_report:
                raise IndexError("Job [{}] does not have a report".format(data['job_id']))

            report_data = json.loads(job_report.data)
            for d in data['keys']:
                old_val = report_data[d['key']]
                action = d.get('action', 'set')
                if action == 'add':
                    d['value'] += old_val

                report_data[d['key']] = d['value']
                job_report.data = json.dumps(report_data)
                session.commit()
                self.log.debug("Report [{}] key '{}' has been set".format(job_report.id, d['key']))

    def bd_md_slave_job_report_key_set(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            job_report = None
            for i in range(0, 3):
                job_report = session.query(masterdb.SlaveJobReport).with_for_update()\
                    .filter(masterdb.SlaveJobReport.job_id==data['job_id']).first()

                if not job_report:
                    self.sleep(1000)

            if not job_report:
                raise IndexError("Job [{}] does not have a report".format(data['job_id']))

            report_data = json.loads(job_report.data)
            old_val = report_data[data['key']]
            action = data.get('action', 'set')
            if action == 'add':
                data['value'] += old_val

            report_data[data['key']] = data['value']
            job_report.data = json.dumps(report_data)
            session.commit()
            self.log.debug("Report [{}] key '{}' has been set".format(job_report.id, data['key']))

    def bd_md_slave_job_pause(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            job = session.query(masterdb.SlaveJob)\
                    .filter(masterdb.SlaveJob.id==data['job_id'])\
                    .first()
            if job:
                if job.stage == 'Paused':
                    job.stage = 'Running'
                    tasks = session.query(masterdb.SlaveTask)\
                            .filter(
                                masterdb.SlaveTask.job_id==job.id,
                                masterdb.SlaveTask.started==False,
                                masterdb.SlaveTask.assigned_slave_id==None
                            )\
                            .all()
                    for task in tasks:
                        task.active = True
                        task.msg = ''
                    session.commit()
                    return

                if not job.completed and not job.error:
                    if 'msg' not in data.keys():
                        data['msg'] = 'Job paused'


                    job.stage = 'Paused'
                    tasks = session.query(masterdb.SlaveTask)\
                            .filter(
                                masterdb.SlaveTask.job_id==job.id,
                                masterdb.SlaveTask.started==False,
                                masterdb.SlaveTask.assigned_slave_id==None
                            )\
                            .all()
                    for task in tasks:
                        task.active = False
                        task.msg = 'Job paused'
                    session.commit()

    def bd_md_slave_job_completed(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            self._slave_job_completed(job_id=data['job_id'], msg=data.get('msg', None), session=session)
            session.commit()

    def bd_md_slave_job_stop(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            job = session.query(masterdb.SlaveJob)\
                    .filter(masterdb.SlaveJob.id==data['job_id'])\
                    .first()

            if job:
                if not job.completed and not job.error:
                    if 'msg' not in data.keys():
                        data['msg'] = 'Job stopped'

                    self._slave_job_error(job=job, msg=data['msg'], task_msg='Stopped!', stage='Stopped', session=session)
                    session.commit()

    def bd_md_slave_job_done(self, data, route_meta):
        pass

    def bd_md_slave_tasks_add(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            for td in data['tasks']:
                self._slave_task_add(td, session=session)
            session.commit()

    def bd_md_slave_task_add(self, data, route_meta):
        timeout = data.get('_timeout', None)
        with self.master_db.scoped_session() as session:
            task = self._slave_task_add(data, session=session)
            session.commit()

            if timeout:
                msg = self.create_local_task_message(
                    "bd.@md.slave.task.check",
                    {"task_id":task.id, "is_assigned": True, "msg": "Task [{}] couldn't find slave before timeout".format(task.id)}
                )
                self.add_local_msg(msg, INBOX_SYS_MSG, delay=timeout)


        #self.master_db.as_json(task)

    def bd_md_slave_task_started(self, data, route_meta, token):
        with self.master_db.scoped_session() as session:
            task = session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==data['task_id'])\
                .first()
            if not task:
                raise KeyError("Task [{}] doesn't  exists".format(data['task_id']))

            if task.assigned_slave_id != token['slave_id']:
                raise Exception("Slave [{}] was trying to start Task [{}] which was assigned to Slave [{}]".format(token['slave_id'], task.id, task.assigned_slave_id))

            if task.started:
                #LOOK AT past code TO SEE WHAT TO PUT HERE
                pass

            task.started = True
            task.time_started = datetime.strptime(data['time_started'], "%Y-%m-%d %H:%M:%S.%f")
            session.commit()

        self.log.debug("Task [{}] was started by Slave [{}]".format(data['task_id'], token['slave_id']))

    def bd_md_slave_task_completed(self, data, route_meta, token):
        te = datetime.strptime(data['time_ended'], "%Y-%m-%d %H:%M:%S.%f")
        self._slave_task_completed(task_id=data['task_id'], time_ended=te)
        self.log.debug("Task [{}] was completed by Slave [{}]".format(data['task_id'], token['slave_id']))

    def bd_md_slave_task_check(self, data, route_meta):
        is_started = data.get('is_started', None)
        is_completed = data.get('is_completed', None)
        is_assigned = data.get('is_assigned', None)

        with self.master_db.scoped_session() as session:
            task = session.query(masterdb.SlaveTask).filter (
                masterdb.SlaveTask.id==data['task_id']
            ).first()
            if not task:
                self.log.error("Task [{}] could not be checked -- Doesn't exist".format(data['task_id']))
                return

            err_msg = data.get('msg', None)

            flag = False

            if is_started != None:
                if task.started != is_started:
                    flag = True

            elif is_completed != None:
                if task.completed != task.completed:
                    flag = True

            elif is_assigned !=None:
                if (task.assigned_slave_id != None) != is_assigned:
                    flag = True

            if flag:
                scope = locals()
                vars_ = ",".join(
                    ["{}={}".format( c, eval(c, scope)) for c in ["is_started", "is_completed", "is_assigned"] if eval(c, scope)!=None]
                )
                self.log.debug("Task [{}] was flagged -- Checking if: {}".format(data['task_id'], vars_))
                self._slave_task_error(task_id=data['task_id'], time_ended=datetime.utcnow(), msg=err_msg, session=session)

    def bd_md_slave_task_error(self, data, route_meta):
        te = None
        if data.get('time_ended', None):
            te = datetime.strptime(data['time_ended'], "%Y-%m-%d %H:%M:%S.%f")

        msg = data.get('msg', None)
        if self._slave_task_error(task_id=data['task_id'], time_ended=te, msg=msg):
            self.log.warning('Task [{}] had an error: "{}"'.format(data['task_id'], msg if msg else "n/a"))

    def bd_md_slave_task_reject(self, data, route_meta, token):
        with self.master_db.scoped_session() as session:
            slave = session.query(masterdb.Slave)\
                .filter(masterdb.Slave.id==token['slave_id'])\
                .first()
            if slave:
                task = session.query(masterdb.SlaveTask)\
                        .filter(and_(masterdb.SlaveTask.id==data['task_id'], masterdb.SlaveTask.assigned_slave_id==slave.id))\
                        .first()

                if not (task.started or task.error):
                    task.assigned_slave_id = None
                    task.active = True
                    task.message = 'Requeued'
                    session.commit()
                    self.log.warning("Task [{}] was rejected by Slave [{}] -- Task is being requeued".format(task.id, slave.id))

    def bd_md_slave_task_schedule_add(self, data, route_meta):
        ######
        #### DOESNT ACCOUNT FOR DIFFERENT WEEKDAY IN DIFFERENT TIMEZONES
        ######
        with self.master_db.scoped_session() as session:
            weekdays = {'monday':0, 'tuesday':1, 'wednesday':2, 'thursday':3, 'friday':4, 'saturday':5, 'sunday':6}
            name = data['name']

            scheduler_group = session.scoped.add_model(masterdb.SchedulerGroup, {'name': name}, commit=True)
            schedulers = []

            if not scheduler_group:
                raise Exception("Could not create scheduler_group")

            if data['type'] == 'scheduler':
                scheduler_payloads = []
                date = datetime.utcnow()
                for day in data['days']:
                    d = datetime.utcnow()

                    payload = {}
                    target_day_idx = weekdays[day]
                    while (target_day_idx!=d.weekday()-1):
                        d+=timedelta(days=1)

                    d = d.replace(hour=int(data['clock'].split(':')[0]), minute=int(data['clock'].split(':')[1]))
                    payload['run_time'] = d
                    payload['frequency_min'] =  60*24*7
                    payload['route'] = data['route']
                    payload['scheduler_group_id'] = scheduler_group.id
                    scheduler = session.scoped.add_model(masterdb.Scheduler, payload)
                    if not scheduler:
                        session.rollback()
                        raise Exception("SOME ISSUE SCHEDULER RETURNED NONE")

                    schedulers.append(scheduler)


            elif data['type'] == 'timer':
                payload = {}

                payload['route']  = data['route']
                payload['data'] = data.get('data', {})
                payload['route_meta'] = data.get('route_meta', {})
                multiplier = 1
                if data['frequency_unit'] == 'minutes':
                    pass
                elif data['frequency_unit'] == 'hours':
                    multiplier = 60
                elif data['frequency_unit'] == 'days':
                    multiplier = 60 * 24
                else:
                    raise ValueError(" Freq unit is not in system: {}".format(data['frequency_unit']))

                freq_min = multiplier * data['frequency']
                run_time = datetime.utcnow() + timedelta(minutes=freq_min)

                if 'repeat' in data.keys():
                    if data['repeat']:
                        payload['frequency_min'] = freq_min
                        payload['run_time'] = run_time
                    else:
                        payload['run_time'] = run_time
                else:
                    payload['run_time'] = run_time

                payload['scheduler_group_id'] = scheduler_group.id

                session.scoped.add_model(masterdb.Scheduler, payload)
            session.commit()

    def bd_md_slave_task_schedule_remove(self, data, route_meta):
        with self.master_db.scoped_session() as session:
            scheduler_group_id = None
            schedulers = []
            scheduler_group_ids = []


            if 'scheduler_group_ids' in data.keys():
                scheduler_group_ids = data['scheduler_group_ids']
            if 'scheduler_group_id' in data.keys():
                scheduler_group_id = data['scheduler_group_id']

            if 'scheduler_id' in data.keys():
                scheduler = session.query(masterdb.Scheduler)\
                        .filter(masterdb.Scheduler.id==data['scheduler_id'])\
                        .first()

                scheduler_group_id = scheduler.scheduler_group_id

            scheduler_group_ids.append(scheduler_group_id)
            for scheduler_group_id in scheduler_group_ids:
                schedulers = session.query(masterdb.Scheduler)\
                        .filter(masterdb.Scheduler.scheduler_group_id==scheduler_group_id)\
                        .all()

                scheduler_group = session.query(masterdb.SchedulerGroup)\
                            .filter(masterdb.SchedulerGroup.id==scheduler_group_id)\
                            .first()
                if scheduler_group:
                    for scheduler in schedulers:
                        session.scoped.delete(scheduler)
                    session.scoped.delete(scheduler_group)
            session.commit()

    @DBWrapper.scoped_session_func(attr='master_db')
    def slave_is_working(self, *, slave_id=None, slave=None, working=False, task_id=None, force_stop=False, session=None):
        if slave_id:
            slave = session.query(masterdb.Slave)\
                .filter(masterdb.Slave.id==slave_id)\
                .first()

        if not slave:
            raise ValueError("Slave [{}] does not exist".format(slave_id))

        if not working:
            if slave.assigned_task_id and force_stop:
                self.stop_slave_global_task(slave.uuid, slave.assigned_task_id, force=force_stop)
            slave.assigned_task_id = None

        else:
            if not task_id:
                raise ValueError("Task id is null")
            slave.assigned_task_id = task_id

        slave.working = working

    def send_slave_global_task(self, uuid, route, data, task_id, job_id, route_meta={}):
        route_meta.update({'task_id': task_id, 'job_id':job_id})
        msg = {
            'route_meta': route_meta,
            'route': 'bd.@sd.task.global.start',
            'data': {
                'route': route,
                'data': data
            }
        }
        self.send_message_to(uuid, msg)

    def stop_slave_global_task(self, uuid, task_id, force=False):
        msg = self.create_local_task_message(
            "bd.@sd.task.global.stop",
            {"task_id":task_id, 'force': force}
        )
        self.send_message_to(uuid, msg, OUTBOX_SYS_MSG)

    def bd_md_Slave_CPv2_redirected(self, data, route_meta, token):
        msg = create_local_task_message(data['route'], data['data'])

        msg['route_meta'] = route_meta
        msg['route_meta']['is_redirect'] = True
        msg['route_meta']['redirect_msg_id'] = data['__redirect_msg_id']
        msg['route_meta']['redirect_sid'] = data['sid']
        msg['route_meta']['redirect_origin'] = token['origin']

        self.inbox.put(msg, priority=INBOX_TASK1_MSG)
        self.alert_CPv2(
            data['__redirect_msg'],
            go_to=None,
            persist=False,
            sids=[data['sid']],
            slave_uuid=token['origin'],
            redirect_msg_id=data['__redirect_msg_id'],
            redirect_resp=True
        )

    def bd_md_IN_Slave_CPv2_resp_times(self, data, route_meta, token):

        out = {}
        cpv2_send_time = data['CPV2_SEND_TIME']
        out['CPV2_SEND_TIME'] = cpv2_send_time

        now = datetime.utcnow()
        r = (now - datetime.strptime(cpv2_send_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds() * 1000
        r = int(r)
        out['MASTER_SEND_TIME'] = str(now)
        out['resp_forward'] = r
        out['slaves'] = self.slaves_recent_resp_times

        self.slaves_recent_resp_times = {}

        msg = create_local_task_message(
            'bd.sd.@CPv2<master.resp.times',
            out
        )
        self.send_message_to(route_meta['origin'], msg)

    def bd_md_IN_Slave_CPv2_job_list(self, data, route_meta, token):
        job_list = [{'route': 'bd.sd.@CBBv1.CPv2.job.list', 'data':{'arg1':'test'}}]
        msg = create_local_task_message(
            'bd.sd.@CPv2<job.list',
            job_list
        )
        return {"list":job_list}
        #self.send_message_to(route_meta['origin'], msg)

    def bd_md_IN_Slave_CPv2_master_configs(self, data, route_meta, token):
        if data.get('configs', False):
            self.log.debug("Configs setting keys: {}".format(data['configs'].keys()))
            self.set_bot_configs(**data['configs'])

            m1 = self.create_local_task_message(
                "@bd.configs.export",
                {}
            )
            self.inbox.put(m1, INBOX_SYS_MSG)


        cfgs = self.get_bot_configs()
        out = {'configs': cfgs}

        msg = create_local_task_message(
            'bd.sd.@CPv2<master.configs',
            out
        )

        self.send_message_to(route_meta['origin'], msg)


        return out

    def bd_md_OUT_Slave_CPv2_master_configs(self, data, route_meta):
        #prob not needed
        cfgs = self.get_master_configs()
        raise NotImplemented

