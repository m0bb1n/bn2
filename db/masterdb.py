from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timedelta
import json
import os

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

Base = declarative_base()


class PermissionGroup (Base):
    __tablename__ = 'PermissionGroup'
    id = Column(Integer, primary_key=True)
    name = Column(String(30), nullable=False)

    job_read = Column(Boolean, nullable=False)
    job_write = Column(Boolean, nullable=False)
    job_execute = Column(Boolean, nullable=False)

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

class User (Base):
    __tablename__ = 'User'
    id = Column(Integer, primary_key=True)
    username = Column(String(20), nullable=False, unique=True)
    password = Column(String(50), nullable=False)
    permission_group_id = Column(ForeignKey('PermissionGroup.id'), nullable=False)

    def __init__(self, payload):
        self.permission_group_id = payload['permission_group_id']
        self.username = payload['username']
        password_hash = payload['password']

        ##hash pwd

        self.password = password_hash


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

class SchedulerGroup(Base):
    #CPV1 should pull data from this model
    __tablename__ = 'SchedulerGroup'
    id = Column(Integer, primary_key=True)
    schedulers = relationship('Scheduler')
    name = Column(String(100), nullable=False)

    def __init__(self, payload):
        self.name = payload['name']

class Scheduler(Base):
    __tablename__ = 'Scheduler'
    id = Column(Integer, primary_key=True)
    frequency_min = Column(Integer) # repeats every x minutes
    run_time = Column(DateTime, nullable=False)
    route = Column(String(100), nullable=False)
    data = Column(Text, default='{}')
    scheduler_group_id = Column(ForeignKey('SchedulerGroup.id'), nullable=False)
    executed = Column(Boolean, default=False)

    def __init__(self, payload):
        self.run_time = payload['run_time']
        self.route = payload['route']
        self.scheduler_group_id = payload['scheduler_group_id']

        cols = payload.keys()
        if 'frequency_min' in cols:
            self.frequency_min = payload['frequency_min']

        if 'data' in cols:
            if type(payload['data']) == type({}):
                payload['data'] = json.dumps(payload['data'])
            self.data = payload['data']

class Slave (Base):
    __tablename__ = 'Slave'
    id = Column(Integer, primary_key=True)
    slave_type_id = Column(ForeignKey('SlaveType.id'))
    first_pulse = Column(DateTime)
    last_pulse = Column(DateTime)
    is_ec2 = Column(Boolean, default=False)
    ec2_instance_id = Column(String(30))
    active = Column(Boolean, default=True)
    init = Column(Boolean, default=False)
    uuid = Column(String(36))
    working = Column(Boolean, default=False)
    assigned_task_id = Column(ForeignKey('SlaveTask.id'), default=None)

    def __init__(self, payload):
        cols = payload.keys()
        self.first_pulse = datetime.utcnow()
        if 'slave_type_id' in cols:
            self.slave_type_id = payload['slave_type_id']

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

        if 'init' in cols:
            self.init = payload['init']

    def free(self):
        self.active = False

    def pulse(self):
        self.last_pulse = datetime.utcnow()

class SlaveType (Base):
    __tablename__ = 'SlaveType'
    id = Column(Integer, primary_key=True)
    model_tag = Column(String(10), nullable=False)
    module_url = Column(String(200))
    name = Column(String(15))

    def __init__(self, payload):
        self.name = payload['name']
        if 'module_url' in payload.keys():
            self.module_url = payload['module_url']
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

class SlaveJob (Base):
    __tablename__ = 'SlaveJob'
    id = Column(Integer, primary_key=True)

    name = Column(String(50))
    created_time = Column(DateTime, nullable=False)
    completed = Column(Boolean, default=False)
    error = Column(Boolean, default=False)
    msg = Column(String(100))
    stage = Column(String(100), default="Queued")
    tasks = relationship("SlaveTask")
    send_alert = Column(Boolean, default=False)
    hidden = Column(Boolean, default=False)
    task_cnt = Column(Integer, default=0)

    priority = Column(Integer, default=0, nullable=False)

    def __init__(self, payload):
        self.name = payload['name']
        cols = payload.keys()
        self.created_time = datetime.utcnow()
        if 'stage' in cols:
            self.stage = payload['stage']

        if 'send_alert' in cols:
            self.send_alert = payload['send_alert']

        if 'priority' in cols:
            self.priority = payload['priority']

        if 'hidden' in cols:
            self.hidden = payload['hidden']


class SlaveTask (Base):
    __tablename__ = 'SlaveTask'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    job_id = Column(ForeignKey('SlaveJob.id'), nullable=False)
    job_ok_on_error = Column(Boolean, default=False)
    priority = Column(Integer, nullable=False, default=1)
    retried_task_id = Column(ForeignKey('SlaveTask.id'), default=None)
    retry_cnt = Column(Integer, default=0)

    task_group_id = Column(ForeignKey('SlaveTaskGroup.id'), nullable=True)

    active = Column(Boolean, default=True) #if task is active for processing (can be false when task chainer has been chained)

    started = Column(Boolean, default=False)
    completed = Column(Boolean, default=False)

    msg = Column(String(100), default="Waiting for available slave")
    error = Column(Boolean, default=False)


    time_created = Column(DateTime, nullable=False)
    time_started = Column(DateTime, nullable=True)
    time_completed = Column(DateTime, nullable=True)

    route = Column(String(100))
    data = Column(Text, default='{}')
    slave_type_id = Column(ForeignKey('SlaveType.id'), nullable=True)
    assigned_slave_id = Column(ForeignKey('Slave.id'))

    def __init__(self, payload):
        self.time_created = datetime.utcnow()
        self.name = payload['name']
        if type(payload['data']) == type({}):
            payload['data'] = json.dumps(payload['data'])
        self.data = payload['data']
        self.route = payload['route']
        self.job_id = payload['job_id']

        cols = payload.keys()
        if 'slave_type_id' in cols:
            self.slave_type_id = payload['slave_type_id']
        else:
            self.error = True
            self.msg = 'Incorrect model id'
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

    def is_error(self, msg=None):
        self.error = True
        self.msg = msg

accessible_models = [
    [Slave, ['id', 'active', 'uuid', 'slave_type_id', 'is_ec2', 'init', 'working', 'assigned_task_id']],
    [Alert, ['id', 'msg', 'go_to', 'viewed', 'time']],
    [SlaveJob, ['id', 'priority', 'stage', 'msg', 'name', 'error', 'completed', 'hidden', 'task_cnt']],
    [SlaveTask,['id', 'completed', 'active', 'error', 'msg', 'job_id', 'task_group_id', 'name', 'time_created', 'time_started', 'time_completed', 'assigned_slave_id']],
    [SlaveTaskGroup,['job_id', 'id', 'total_cnt', 'completed_cnt', 'error_cnt']],
    [SchedulerGroup,['id', 'name']],
    [Scheduler, ['id','frequency_min', 'run_time','route','data','scheduler_group_id']],
    [SlaveType, ['id', 'name']]
]

