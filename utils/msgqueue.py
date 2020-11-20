
import multiprocessing as multiprocess
import threading
import os
import queue as Queue
from queue import Full

INBOX_SYS_CRITICAL_MSG = 0
INBOX_SYS_CRIT_MSG = 0

INBOX_SYS_MSG = 1
INBOX_BLOCKING_MSG = 2
INBOX_TASK1_MSG = 3
INBOX_TASK2_MSG = 4

OUTBOX_SYS_MSG = 0
OUTBOX_TASK_MSG = 1

def create_global_task_message(self, route, data, *, task_id=None, job_id=None, route_meta=None, **kwargs):
    if not (task_id and job_id):
        raise ValueError("Either Task [{}] or Job [{}] is null")
    gdata = {
        'route': 'bd.@sd.task.global.start',
        'data': {
            'route': route,
            'data': data
        }
    }
    if not route_meta:
        route_meta = create_local_route_meta(**kwargs)

    route_meta.update({'task_id':task_id, 'job_id':job_id})

    return create_local_task_message(
        route, gdata, route_meta=route_meta
    )



def safe_copy_route_meta(org, new, exclude=[], include=[]):
    keys = org.keys()
    if not len(include):
        include = keys
    for inc_k in include:
        if inc_k in keys and not inc_k in exclude:
            new[inc_k] = org[inc_k]

def create_local_route_meta(*, is_process=False, callback=None, origin=None, token=None, local_route_secret=None):
    _type = 'default'
    if is_process:
        _type = 'process'

    return {'type': _type, 'origin':origin, 'token':token, "local_route_secret":local_route_secret, 'callback': callback}

def create_local_task_message(route, data, *, route_meta=None, **kwargs):
    msg = {'route': route, 'data':data}


    meta = create_local_route_meta(**kwargs)
    if route_meta:
        safe_copy_route_meta(route_meta, meta)

    msg['route_meta'] = meta

    return msg


class ChannelQueue (object):
    queues = None
    def __init__(self, channels, manager=None):

       obj = multiprocess.Queue
       self.queues = {}
       if manager:
        obj = manager.Queue

        for channel in channels:
            self.queues[channel] = obj()

    def get(self, channel):
        return self.queues[channel].get_nowait()

    def put(self, item, channel):
        self.queues[channel].put(item)

class PriorityQueue(object):
    queues = None
    q = None
    def __init__(self, total_priorities, manager=None):
        obj = multiprocess.Queue
        self.queues = []
        if manager:
            obj = manager.Queue
        self.total_priorities = total_priorities
        for i in range(0, total_priorities):
            self.queues.append(obj())



    def get(self, priority=None, remove=True, get_priority=False, get_all=False):
        item = None
        items = []
        if (priority != None):
            try:
                if remove:
                    item = self.queues[priority].get_nowait()
                else:
                    item = self.queues[priority].get_nowait()
                    self.put_front(item, priority)
            except Queue.Empty:
                pass
        else:
            priority = 0
            for priority in range(0, self.total_priorities):
                item = self.get(priority, remove=remove)

                if get_all and item:
                    items.append(item)
                    priority-=1

                elif item:
                    break


        if get_all:
            return items

        if get_priority:
            return item, priority

        return item

    def put(self, item, priority=None):
        if priority==None:
            priority= self.total_priorities-1

        self.queues[priority].put(item)

    def put_front(self, item, priority):
        q = self.queues[priority]
        assert not q._closed, "Queue {0!r} has been closed".format(q)
        if not q._sem.acquire(True, None):
            raise Full

        with q._notempty:
            if q._thread is None:
                q._start_thread()
            q._buffer.appendleft(item)

            q._notempty.notify()
