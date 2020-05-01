from bn2.utils.msgqueue import \
    create_local_task_message, create_global_task_message, \
    INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, \
    INBOX_TASK2_MSG, OUTBOX_TASK_MSG, OUTBOX_SYS_MSG
from bn2.comms.server import BotServerFactory
from twisted.internet import reactor, task
from bn2.drivers.botdriver import BotDriver

from sqlalchemy import func, desc, and_, or_
from bn2.db import db
from bn2.db import masterdb
import os
from datetime import datetime, timedelta

class MasterDriver (BotDriver):

    def __init__(self, config):
        super(MasterDriver, self).__init__(config)
        self.__bd_md_route_mappings = {
            'bd.@md.slave.lost': self.bd_md_slave_lost,
            'bd.@md.slave.connect': self.bd_md_slave_connect,
            'bd.@md.slave.register': self.bd_md_slave_register,
            'bd.@md.slave.pulse': self.bd_md_slave_pulse,

            'bd.@md.Slave.PV1.forwarded': self.bd_md_Slave_PV1_forwarded,

            'bd.@md.slave.job.add': None,
            'bd.@md.slave.job.remove': None,
            'bd.@md.slave.job.edit': None,
            'bd.@md.slave.job.completed': None,
            'bd.@md.slave.job.stop': None,
            'bd.@md.slave.job.error': None,
            'bd.@md.slave.job.pause': None,

            'bd.@md.slave.task.add': self.bd_md_slave_task_add,
            'bd.@md.slave.task.remove': None,
            'bd.@md.slave.task.error': None,
            'bd.@md.slave.task.started': self.bd_md_slave_task_started,
            'bd.@md.slave.task.completed': self.bd_md_slave_task_completed,
            'bd.@md.slave.task.stopped': None
        }

        self.add_route_mappings(self.__bd_md_route_mappings)
        self.add_check_func(self.check_slave_tasks, 1000)


        db_fn = '/home/den0/Programs/bn2/bn2/master.db'
        db_exists = os.path.exists(db_fn)
        self.master_db = db.DBWrapper(db_fn, masterdb.Base, 'sqlite', create=not db_exists, scoped_thread=True)
        if not db_exists:
            args = [
                {'name': 'ControlPanelv2', 'model_tag': 'CPV2'}
            ]
            for a in args:
                self.master_db.add(masterdb.SlaveType, a)

        else:
            self.master_db.session.rollback()
            slaves = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.active==True).all()
            for slave in slaves:
                slave.active = False
                slave.assigned_task_id = None
                slave.working = False


        jobs = self.master_db.session.query(masterdb.SlaveJob)\
            .filter(
                masterdb.SlaveJob.error==False,
                masterdb.SlaveJob.completed==False
            )\
            .all()

        for job in jobs:
            self._slave_job_error(job=job, msg='Master exited abruptly before jobs completed')


    def run_comms (self): #this is for masterdriver
        self.heartbeat.__track_process__(name='MasterDriver Comms', route='@bd.comms.launch')
        self.factory = BotServerFactory(self.uuid, self.inbox, self.log)
        reactor.listenTCP(5500, self.factory)
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse)
        cp = self.comms_pulse_cb.start(10)
        self.init_outbox_callback()
        reactor.run()

    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        payload = {'target_uuid':uuid, 'data':msg}
        try:
            self.outbox.put(payload, priority)
        except Exception as e:
            self.report_issue(str(e), 'bd.md.send_message_to', ERROR_LVL=True)


    def send_message_to_all(self):
        pass


    def check_slave_tasks(self):
        with self.master_db.scoped_session() as session:
            tasks = session.query(masterdb.SlaveTask)\
                .filter(
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
                ).all()


            if not slaves:
                return

            found_slave_type_ids = [slave.slave_type_id for slave in slaves]
            ready_slaves = [slave for slave in slaves if not slave.assigned_task_id]

            waiting_task_ids = []
            for job in jobs:
                job_tasks = job_map[job.id]
                for task in job_tasks:
                    found_ready_slave = False
                    for i, slave in enumerate(ready_slaves):
                        if slave.slave_type_id == task.slave_type_id:
                            found_ready_slave = True
                            slave = ready_slaves.pop(i)

                            task.assigned_slave_id = slave.id
                            task.msg = 'Assigned to slave [{}]'.format(slave.id)
                            #slave.working = True
                            slave.assigned_task_id = task.id

                            if job.stage == 'Queued':
                                job.stage = 'Running'

                            session.commit()
                            self.send_slave_global_task(slave.uuid, task.route, task.data, task.id, task.job_id)
                            self.log.info('Assigned Task [{}] to Slave [{}]'.format(task.id, slave.id), path='bd.@md.check.slave.tasks')
                            break

                    if not found_ready_slave: #either means no ready slaves or non online
                        if not task.slave_type_id in found_slave_type_ids:
                            self.log.error("Task [{}] needs slave type id [{}]".format(task.id, task.slave_type_id), path='bd.@md.check.slave.tasks')
                        else:
                            waiting_task_ids.append(task.id)
            if waiting_task_ids:
                self.log.warning("Task {} are waiting for available slaves".format(waiting_task_ids), path='bd.@md.check.slave.tasks')



    def _slave_task_add(self, data):
        model_tag = self.get_model_tag_from_route(data['route'])
        if not model_tag in ['bd', 'md', 'sd'] and not 'slave_type_id' in data.keys():
            slave_type_id = self.master_db.session.query(masterdb.SlaveType.id)\
                .filter(masterdb.SlaveType.model_tag==model_tag)\
                .first()

            if slave_type_id:
                slave_type_id = slave_type_id[0]
                data['slave_type_id'] = slave_type_id
        task = None
        if not 'job_id' in data.keys():
            job_data = {"job_data":{"name": "Anon Job (hidden will be true)", 'hidden': False}, "tasks_data": [data]}
            job, tasks = self._slave_job_add(job_data)
            task = tasks[0]

        else:
            task = self.master_db.add(masterdb.SlaveTask, data)
            if task.error:
                pass

            job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()
            job.task_cnt+=1
            self.master_db.session.commit()

        return task

    def _slave_task_completed(self, *, task_id=None, task=None, time_completed):
        if task_id:
            task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==task_id)\
                .first()

        if not task:
            raise ValueError("Task [{}] doesn't exist".format(task_id))
        task.completed = True
        task.time_completed = time_completed
        self.master_db.session.commit()

    def _slave_job_completed(self, *, job=None, job_id=None, msg='OK'):
        if job_id:
            job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==job_id)\
                .first()

        if not job:
            raise ValueError("Job [{}] doesn't exist".format(job_id))

        job.completed = True
        job.stage = 'Done!'
        job.msg = msg
        self.master_db.session.commit()


    def _slave_job_add(self, data):
        job = self.master_db.add(masterdb.SlaveJob, data['job_data'])
        tasks = []
        if 'tasks_data' in data.keys():
            for task in data['tasks_data']:
                task['job_id'] = job.id
                task_obj = self._slave_task_add(task)
                tasks.append(task_obj)
        return job, tasks

    def _slave_task_error(self, *, task_id=None, task=None, msg='Task had an unexpected error', check_job=True):
        if task_id:
            task = self.master_db.session.query(masterdb.SlaveTask)\
            .filter(masterdb.SlaveTask.id==task_id)\
            .first()

        if not task:
            raise ValueError("Task [{}] doesn't exist".format(task_id))

        if task.started and (not task.completed or not task.error):
            #self._stop_slave_task(task=task, slave_id=task.assigned_slave_id)
            pass

        task.error = True
        task.msg = msg
        self.master_db.session.commit()

        RETRY_OK = True
        if task.job_id:
            job = self.master_db.session.query(masterdb.SlaveJob)\
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
                    parent_task = self.msater_db.session.query(masterdb.SlaveTask)\
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
                retrying_task = self.master_db.add(masterdb.SlaveTask, task_retry_payload)

                self.master_db.session.commit()

                #retries left so finished func
                return

            elif task.retry_cnt==0:
                #no retries left
                check_job = True
                task.job_ok_on_error = False
                pass

        if task.job_id and check_job:
            if not task.job_ok_on_error:
                err_msg = "Job was stopped because task [{}] failed max tries".format(task.id)
                self._slave_job_error(job_id=task.job_id, msg=err_msg, stage='Failed Retries')


            elif task.task_group_id:
                #handle task_group
                pass

        self.master_db.session.commit()


    def _slave_job_error(self, *, job_id=None, job=None, msg='Job had an unexpected error', task_msg=None, stage='Failed'):
        if not task_msg:
            task_msg = msg

        if job_id:
            job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==job_id)\
                .first()

        if not job:
            raise ValueError("Job [{}] doesn't exist".format(job_id))

        if job:
            job.error = True
            job.completed = False
            job.msg = msg
            job.stage = stage
            self.master_db.session.commit()

            for job_task in job.tasks:
                if not job_task.error:
                    self._slave_task_error(task_id=job_task.id, msg=task_msg, check_job=False)

    def bd_md_slave_pulse(self, data, route_meta):
        self.log.debug("GOT PULSE FROM SLAVE {}".format(route_meta['origin']))

    def bd_md_slave_lost(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave)\
            .filter(masterdb.Slave.uuid==data['uuid'])\
            .first()
        if slave:
            self.log.warning("Slave [{}] has been lost".format(slave.id))
            slave.active = False
            if slave.is_ec2:
                #terminate ec2 here
                pass
            if slave.working:
                slave.working = False
                if slave.assigned_task_id:
                    err_msg = "Slave [{}] was lost unexpectedly".format(slave.id)

                    self._slave_task_error(task_id=slave.assigned_task_id, msg=err_msg)

            self.master_db.session.commit()

    def bd_md_slave_connect(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.uuid==route_meta['origin']).first()
        if not slave:
            self.log.debug('Slave {} is requesting to connect'.format(route_meta['origin']))

            msg = create_local_task_message (
                'bd.@sd.slave.auth',
                {
                    'master_uuid': self.uuid,
                    'pk': 'private_key'
                }
            )

            self.send_message_to(route_meta['origin'], msg, priority=OUTBOX_SYS_MSG)
        else:
            self.log.debug('Reestablishing connection with Slave [{}]'.format(slave.id))

            slave.active = True
            self.master_db.session.commit()
            ###look at github for more info


    def bd_md_slave_register(self, data, route_meta):
        self.log.debug('Slave {} sent registering information {}'.format(route_meta['origin'], data))

        slave_type = self.master_db.session.query(masterdb.SlaveType)\
            .filter(masterdb.SlaveType.model_tag==data['model_tag'])\
            .first()
        if slave_type:
            data['slave_type_id'] = slave_type.id
        else:
            raise ValueError(
                "Model tag '{}' is not in Slave Type Table".format(data['model_tag'])
            )
        data['is_ec2'] = False
        data['first_pulse'] = datetime.utcnow()
        data['last_pulse'] = datetime.utcnow() + timedelta(seconds=15)
        slave = self.master_db.add(masterdb.Slave, data)

        if slave:
            self.log.info("Registered Slave [{}] type '{}'".format(slave.id, slave_type.name))
        else:
            raise ValueError("Slave could not be registered")

    def bd_md_slave_job_add(self, data, route_meta):
        pass

    def bd_md_slave_job_remove(self, data, route_meta):
        pass

    def bd_md_slave_job_edit(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
            .filter(masterdb.SlaveJob.id==data['job_id'])\
            .first()

        data.pop('job_id')
        for k in data.keys():
            if not k in []: #restricted columns
                setattr(job, k, data['k'])

    def bd_md_slave_job_error(self, data, route_meta):
        msg = data.get('msg')
        pass

    def bd_md_slave_job_pause(self, data, route_meta):
        pass

    def bd_md_slave_job_stop(self, data, route_meta):
        pass

    def bd_md_slave_job_done(self, data, route_meta):
        pass

    def bd_md_slave_task_add(self, data, route_meta):
        self._slave_task_add(data)


    def bd_md_slave_task_started(self, data, route_meta):
        task = self.master_db.session.query(masterdb.SlaveTask)\
            .filter(masterdb.SlaveTask.id==data['task_id'])\
            .first()
        if not task:
            raise KeyError("Task [{}] doesn't  exists".format(data['task_id']))

        if task.started:
            #LOOK AT GITUB TO SEE WHAT TO PUT HERE
            pass

        task.started = True
        task.time_started = datetime.strptime(data['time_started'], "%Y-%m-%d %H:%M:%S.%f")
        self.master_db.session.commit()


    def bd_md_slave_task_completed(self, data, route_meta):
        task = self.master_db.session.query(masterdb.SlaveTask)\
            .filter(masterdb.SlaveTask.id==data['task_id'])\
            .first()
        if not task:
            raise KeyError("Task [{}] doesn't  exists".format(data['task_id']))

        task.completed = True
        task.time_completed = datetime.strptime(data['time_completed'], "%Y-%m-%d %H:%M:%S.%f")
        self.master_db.session.commit()
        self.slave_is_working(task.assigned_slave_id, False)

        #check task.job_id to see if its done
        pass

    def bd_md_slave_task_error(self, data, route_meta):
        pass

    def slave_is_working(self, slave_id, working):
        #maybe allow slave_id or slave obj
        slave = self.master_db.session.query(masterdb.Slave)\
            .filter(masterdb.Slave.id==slave_id)\
            .first()

        slave.working = working
        if not working:
            slave.assigned_task_id = None

        self.master_db.session.commit()

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


    def bd_md_Slave_PV1_forwarded(self, data, route_meta):
        msg = create_local_task_message(data['route'], data['data'])
        self.inbox.put(msg, priority=INBOX_TASK1_MSG)

