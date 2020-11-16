from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean, event
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, relationship, validates
from datetime import datetime, timedelta
import json
import os

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

Base = declarative_base()


def update_record(mapper, connection, target):
    target._update_time = datetime.utcnow()

class UpdateBase (object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


    id = Column(Integer, primary_key=True)
    _update_time = Column(DateTime, nullable=False)

    def __init__(self):
        _update_time = Column(DateTime, nullable=False)

event.listen(UpdateBase, 'before_update', update_record, propagate=True)

class PermissionGroup (Base):
    __tablename__ = 'PermissionGroup'
    id = Column(Integer, primary_key=True)
    name = Column(String(30), nullable=False)

    job_read = Column(Boolean, nullable=False)
    job_write = Column(Boolean, nullable=False)
    job_execute = Column(Boolean, nullable=False)
    #job

    master_read = Column(Boolean, nullable=False)
    master_write = Column(Boolean, nullable=False)
    master_execute = Column(Boolean, nullable=False)

    slave_read = Column(Boolean, nullable=False)
    slave_write = Column(Boolean, nullable=False)
    slave_execute = Column(Boolean, nullable=False)

    warehouse_read = Column(Boolean, nullable=False)
    warehouse_write = Column(Boolean, nullable=False)
    warehouse_execute = Column(Boolean, nullable=False)


    def __init__(self, payload):
        self.name = payload['name']

        self.job_read = payload['job_read']
        self.job_write = payload['job_write']
        self.job_execute = payload['job_execute']


        self.master_read = payload['master_read']
        self.master_write = payload['master_write']
        self.master_execute = payload['master_execute']

        self.slave_read = payload['slave_read']
        self.slave_write = payload['slave_write']
        self.slave_execute = payload['slave_execute']

        self.warehouse_read = payload['warehouse_read']
        self.warehouse_write = payload['warehouse_write']
        self.warehouse_execute = payload['warehouse_execute']

"""
class RouteLevelGroup (Base):
    __tablename__ = 'RouteLevelGroup'
    id = Column(Integer, primary_key=True)

    def __init__(self, payload):
        self.bitmask = payload['bitmask']
"""
class User (Base):
    __tablename__ = 'User'
    id = Column(Integer, primary_key=True)
    username = Column(String(20), nullable=False, unique=True)
    password = Column(String(50), nullable=False)
    bitmask = Column(Integer, nullable=False)
    levels = Column(Text, nullable=False)
    #use a permission bit number and map by keys to make dyani bitmask

    def __init__(self, payload):
        self.username = payload['username']
        self.bitmask = payload['bitmask']
        self.levels = payload['levels']
        password_hash = payload['password']

        ##hash pwd

        self.password = password_hash

    def verify(self, password):
        return self.password == password


class Alert (Base):
    __tablename__ = 'Alert'
    id = Column(Integer, primary_key=True)
    msg = Column(String(100), nullable=False)
    go_to = Column(String(50), nullable=True)
    viewed = Column(Boolean, default=False)
    time = Column(DateTime, nullable=False)
    color = Column(String(30), nullable=True)

    def __init__(self, payload):
        self.msg = payload['msg']
        self.time = datetime.utcnow()

        cols = payload.keys()
        if 'go_to' in cols:
            self.go_to = payload['go_to']
        if 'color' in cols:
            self.color = payload['color']


class SlaveTaskGroup (Base):
    __tablename__ = "SlaveTaskGroup"
    id = Column(Integer, primary_key=True)
    job_id = Column(ForeignKey('SlaveJob.id'), nullable=False)
    completed_cnt = Column(Integer, default=0)
    error_cnt = Column(Integer, default=0)
    total_cnt = Column(Integer, nullable=False)
    cb_route = Column(String(100), nullable=False)
    cb_name = Column(String(75), nullable=False)
    cb_data_tag = Column(String(100), nullable=True)
    grouped = Column(Boolean, default=False)


    def __init__(self, payload):
        self.total_cnt = payload['total_cnt']
        self.job_id = payload['job_id']
        self.cb_route = payload['cb_route']
        self.cb_name = payload['cb_name']
        if 'cb_data_tag' in payload.keys():
            self.cb_data_tag = payload['cb_data_tag']

class SchedulerGroup(UpdateBase, Base):
    #CPV1 should pull data from this model
    __tablename__ = 'SchedulerGroup'
    id = Column(Integer, primary_key=True)
    schedulers = relationship('Scheduler')
    name = Column(String(100), nullable=False)
    active = Column(Boolean, default=True)

    def __init__(self, payload):
        self.name = payload['name']

        self._update_time = datetime.utcnow()


class Scheduler(Base):
    __tablename__ = 'Scheduler'
    id = Column(Integer, primary_key=True)
    frequency_min = Column(Integer) # repeats every x minutes
    run_time = Column(DateTime, nullable=False)
    route = Column(String(100), nullable=False)
    data = Column(Text, default='{}')
    route_meta = Column(Text, nullable=None)
    scheduler_group_id = Column(ForeignKey('SchedulerGroup.id'), nullable=False)
    executed = Column(Boolean, default=False)


    def __init__(self, payload):
        self.run_time = payload['run_time']
        self.route = payload['route']
        self.route_meta = json.dumps(payload['route_meta'])
        self.scheduler_group_id = payload['scheduler_group_id']

        cols = payload.keys()
        if 'frequency_min' in cols:
            self.frequency_min = payload['frequency_min']

        if 'data' in cols:
            if type(payload['data']) == type({}):
                payload['data'] = json.dumps(payload['data'])
            self.data = payload['data']

class Slave (UpdateBase, Base):
    __tablename__ = 'Slave'
    id = Column(Integer, primary_key=True)
    slave_type_id = Column(ForeignKey('SlaveType.id'))
    first_pulse = Column(DateTime)
    last_pulse = Column(DateTime)
    is_ec2 = Column(Boolean, default=False)
    ec2_instance_id = Column(String(30))
    active = Column(Boolean, default=True)
    is_init = Column(Boolean, default=True)
    uuid = Column(String(36))
    working = Column(Boolean, default=False)
    assigned_task_id = Column(ForeignKey('SlaveTask.id'), default=None)
    ip = Column(String(16))


    def __init__(self, payload):
        cols = payload.keys()
        self.first_pulse = datetime.utcnow()
        self.slave_type_id = payload['slave_type_id']


        if 'ip' in cols:
            self.ip = payload['ip']

        if 'is_ec2' in cols:
            self.is_ec2 = payload['is_ec2']
            if self.is_ec2:
                self.ec2_instance_id = payload['ec2_instance_id']


        if 'active' in cols:
            self.active = payload['active']

        if 'uuid' in cols:
            self.uuid = payload['uuid']


        if 'first_pulse' in cols:
            self.first_pulse = payload['first_pulse']
        if 'last_pulse' in cols:
            self.last_pulse = payload['last_pulse']

        if 'is_init' in cols:
            self.is_init = payload['is_init']
        self._update_time = datetime.utcnow()

    def free(self):
        self.active = False

    def pulse(self):
        self.last_pulse = datetime.utcnow()

class SlaveType (Base):
    __tablename__ = 'SlaveType'
    id = Column(Integer, primary_key=True)
    model_tag = Column(String(10), nullable=False, unique=True)
    module_url = Column(String(200))
    name = Column(String(50))
    cfgs_url = Column(Text, nullable=True)
    slave_url = Column(Text, nullable=True)

    def __init__(self, payload):
        cols = payload.keys()
        self.name = payload['name']
        if 'module_url' in cols:
            self.module_url = payload['module_url']

        if 'cfgs_url' in cols:
            self.cfgs = payload['cfgs_url']
        if 'slave_url' in cols:
            self.slave_url = payload['slave_url']

        self.model_tag = payload['model_tag']

class SlaveTaskChainer (Base):
    __tablename__ = 'SlaveTaskChainer'
    id = Column(Integer, primary_key=True)

    parent_task_id = Column(ForeignKey('SlaveTask.id'), nullable=False)
    parent_task = relationship("SlaveTask", foreign_keys=[parent_task_id])

    child_task_id = Column(ForeignKey('SlaveTask.id'), nullable=False)
    child_task = relationship("SlaveTask", foreign_keys=[child_task_id])

    run_child_with_error = Column(Boolean, default=False)
    chained = Column(Boolean, nullable=True) #if the following task was executed = true , if it fails = false
    #args = Column(Text, default='{}')


    def __init__(self, payload):
        self.parent_task_id = payload['parent_task_id']
        self.child_task_id = payload['child_task_id']

        cols = payload.keys()

        if 'run_child_with_error' in cols:
            self.run_child_with_error = payload['run_child_with_error']

        #self.args = payload['args']

class SlaveJobReport (UpdateBase, Base):
    __tablename__ = 'SlaveJobReport'
    id = Column(Integer, primary_key=True)
    job_id = Column(ForeignKey('SlaveJob.id'), nullable=False)
    type = Column(String(10), default='plaintext')
    content = Column(Text, nullable=True)
    data = Column(Text, default="{}")
    skeleton = Column(Text, nullable=True)

    def __init__(self, payload):
        self._update_time = datetime.utcnow()
        self.job_id = payload['job_id']
        self.type = payload['type']

        data = payload.get('data', '{}')
        if type(data) == dict:
            self.data = json.dumps(data)
        skeleton = payload.get('skeleton', None)
        if skeleton:
            skeleton = skeleton.replace('{', '{{').replace('}', "}}")
            skeleton = skeleton.replace("<<", '{').replace('>>', '}')
        self.skeleton = skeleton

    @validates('msg')
    def _validates(self, k, v):
        max_len = getattr(self.__class__, k).prop.columns[0].type.length
        if v and len(v) > max_len-3:
            return v[:max_len]+'...'
        return v


class SlaveJob (UpdateBase, Base):
    __tablename__ = 'SlaveJob'
    id = Column(Integer, primary_key=True)

    job_report_id = Column(ForeignKey('SlaveJobReport.id'))

    name = Column(String(100))
    created_time = Column(DateTime, nullable=False)
    completed = Column(Boolean, default=False)
    error = Column(Boolean, default=False)
    msg = Column(String(100))
    stage = Column(String(100), default="Queued")
    tasks = relationship("SlaveTask")
    send_alert = Column(Boolean, default=False)
    hidden = Column(Boolean, default=False)
    task_cnt = Column(Integer, default=1)

    priority = Column(Integer, default=0, nullable=False)

    def __init__(self, payload):

        self._update_time = datetime.utcnow()
        self.name = payload['name']
        cols = payload.keys()
        self.created_time = datetime.utcnow()

        if 'job_report_id' in cols:
            self.job_report_id = payload['job_report_id']

        if 'stage' in cols:
            self.stage = payload['stage']

        if 'send_alert' in cols:
            self.send_alert = payload['send_alert']

        if 'priority' in cols:
            self.priority = payload['priority']

        if 'hidden' in cols:
            self.hidden = payload['hidden']

    @validates('msg')
    def _validates(self, k, v):
        max_len = getattr(self.__class__, k).prop.columns[0].type.length
        if v and len(v) > max_len:
            return v[:max_len]
        return v


class SlaveTask (UpdateBase, Base):
    __tablename__ = 'SlaveTask'

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    job_id = Column(ForeignKey('SlaveJob.id'), nullable=False)
    job_ok_on_error = Column(Boolean, nullable=False, default=False)
    priority = Column(Integer, nullable=False, default=1)
    retried_task_id = Column(ForeignKey('SlaveTask.id'), default=None)
    retry_cnt = Column(Integer, default=-1)

    task_group_id = Column(ForeignKey('SlaveTaskGroup.id'), nullable=True)

    active = Column(Boolean, default=True) #if task is active for processing (can be false when task chainer has been chained)

    started = Column(Boolean, default=False)
    completed = Column(Boolean, default=False)

    msg = Column(String(250), default="Waiting for available slave")
    error = Column(Boolean, default=False)


    time_created = Column(DateTime, nullable=False)
    time_started = Column(DateTime, nullable=True)
    time_ended = Column(DateTime, nullable=True)

    route = Column(String(100))
    data = Column(Text, default='{}')
    route_meta = Column(Text, nullable=False)
    slave_type_id = Column(ForeignKey('SlaveType.id'), nullable=True)
    assigned_slave_id = Column(ForeignKey('Slave.id'))

    def __init__(self, payload):
        self.time_created = datetime.utcnow()
        self.name = payload['name']
        if type(payload['data']) == type({}):
            payload['data'] = json.dumps(payload['data'])
        self.data = payload['data']
        self.route = payload['route']
        self.route_meta = json.dumps(payload.get('route_meta', {}))
        self.job_id = payload['job_id']

        cols = payload.keys()
        if 'slave_type_id' in cols:
            self.slave_type_id = payload['slave_type_id']
        else:
            self.error = True
            self.msg = 'Model tag does not exist'
            self.active = False

        if 'job_ok_on_error' in cols:
            self.job_ok_on_error = payload['job_ok_on_error']

        if 'task_group_id' in cols:
            self.task_group_id = payload['task_group_id']

        if 'priorty' in cols:
            self.priority = payload['priority']

        if 'retried_task_id' in cols:
            self.retried_task_id = payload['retried_task_id']
            self.retry_cnt = payload['retry_cnt']

        if 'retry_cnt' in cols:
            self.retry_cnt = payload['retry_cnt']

        self._update_time = datetime.utcnow()
    def is_error(self, msg=None):
        self.error = True
        self.msg = msg

    @validates('msg')
    def _validates(self, k, v):
        max_len = getattr(self.__class__, k).prop.columns[0].type.length
        if v and len(v) > max_len:
            return v[:max_len]
        return v

accessible_models = [
    [Slave, ['id', 'active', 'uuid', 'slave_type_id', 'is_ec2', 'is_init', 'working', 'assigned_task_id']],
    [Alert, ['id', 'msg', 'go_to', 'viewed', 'time']],
    [SlaveJob, ['id', 'priority', 'stage', 'msg', 'name', 'error', 'completed', 'hidden', 'task_cnt']],
    [SlaveTask,['id', 'completed', 'active', 'error', 'msg', 'job_id', 'task_group_id', 'name', 'time_created', 'time_started', 'time_ended', 'assigned_slave_id']],
    [SlaveTaskGroup,['job_id', 'id', 'total_cnt', 'completed_cnt', 'error_cnt']],
    [SchedulerGroup,['id', 'name']],
    [Scheduler, ['id','frequency_min', 'run_time','route','data','scheduler_group_id']],
    [SlaveType, ['id', 'name']]
]

