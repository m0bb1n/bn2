from multiprocessing import Queue
import os
from datetime import datetime, timedelta
from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_MSG, INBOX_SYS_CRITICAL_MSG
import sys
from time import sleep
import psutil

import threading
import ctypes



class StoppableThread(threading.Thread):
    def __init__(self, name=None, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)

    def get_id(self):
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id

    def stop(self, throw=False):
        try:
            target_tid = self.get_id()
            if target_tid not in {thread.ident for thread in threading.enumerate()}:
                raise ValueError('Invalid thread object, cannot find thread identity among currently active threads.')

            affected_count = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), ctypes.py_object(SystemExit))

            if affected_count == 0 and throw:
                raise ValueError('Invalid thread identity, no thread has been affected.')
            elif affected_count > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), ctypes.c_long(0))
                raise SystemError("PyThreadState_SetAsyncExc failed, broke the interpreter state.")
        except Exception as e:
            if throw:
                raise e


        """

        thread_id = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
              ctypes.py_object(SystemExit))

        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            print('Exception raise failure')
        """
class Heartbeat (object):
    q = None
    pids = {}
    def __init__(self, queue, outbox, grace=30, parent_pid=None, usage_send_interval=None):
        self.q = queue
        self.grace = grace
        self.active = False
        self.parent_pid = parent_pid
        self.outbox = outbox
        if usage_send_interval == 0:
            usage_send_interval = None
        self.usage_send_interval = usage_send_interval

    def KILLALL(self):
        self.q.put('KILLALL',0)

    def kill_pid(self, pid):
        os.system("kill -9 {}".format(pid))

    def set_logger(self, logger):
        self.log = logger
        self.log.set_path('@bd.watchdog')

    def get_next_usage_timer(self):
        return datetime.utcnow()+timedelta(seconds=self.usage_send_interval)

    def __killall(self):
        pids = list(self.pids)
        for p in pids: #kills all others pids before
            if p != self.parent_pid:
                self.kill_pid(p)

        self.kill_pid(self.parent_pid)
        self.kill_pid(os.getpid())
        sys.exit(1)


    def watchdog(self):
        self.log.debug("Starting Watchdog pid: {}".format(os.getpid()))

        USAGE_INTERVAL = None
        if self.usage_send_interval:
            USAGE_INTERVAL =  self.get_next_usage_timer()

        LISTEN = True
        while LISTEN:
            LISTEN = self.listen()
            pids = list(self.pids)

            SEND_USAGE = False
            if USAGE_INTERVAL:
                SEND_USAGE = USAGE_INTERVAL < datetime.utcnow()

            if len(pids):
                if not self.active: #activates heartbeat to begin tracking and exit if no more pids
                    self.active = True


                bot_usage = []
                for pid in pids:
                    pid_info = self.pids[pid]
                    if pid_info['pulse'].is_expired(datetime.utcnow()):
                        if pid_info['pulse'].flag:
                            name = pid_info['name']
                            route = pid_info['meta']['route']
                            data = pid_info['meta']['data']
                            relaunch = pid_info['meta']['relaunch']
                            self.log.critical("Process '{}' [{}] is inactive - killing".format(name, pid))

                            if self.parent_pid == pid:
                                self.log.critical("Main Process '{}' [{}] is inactive - Killing Everything".format(name,pid))
                                self.__killall() #exits here
                                return


                            self.kill_pid(pid)
                            del self.pids[pid]

                            if route:
                                dead_msg = create_local_task_message('@bd.process.died', {'route':route,'data':data, 'relaunch':relaunch})
                                self.outbox.put(dead_msg, INBOX_SYS_CRITICAL_MSG)

                        pid_info['pulse'].flag = True

                    if SEND_USAGE:
                        usage = self.get_process_usage(pid)
                        usage['name'] = pid_info['name']
                        bot_usage.append(usage)

                if SEND_USAGE:
                    if len(bot_usage):
                        bot_usage[0]['cpu_freq'] = psutil.cpu_freq()[0]

                    msg = create_local_task_message('@bd.usage', bot_usage)
                    self.outbox.put(msg,INBOX_SYS_CRITICAL_MSG)
                    USAGE_INTERVAL =  self.get_next_usage_timer()



            else:
                if self.active:
                    self.log.info("No pids to track... Exiting")
                    break

    def listen(self):
        pulse = None
        pulse = self.q.get()

        if type(pulse) == type({}): #its a new pid to add
            pid = pulse['pulse'].pid
            self.log.info("Tracking new Process '{}' Pid: {}".format(pulse['name'], pid))
            self.pids[pid] = {}
            self.pids[pid]['pulse'] = pulse['pulse']
            self.pids[pid]['name'] = pulse['name']
            self.pids[pid]['meta'] = {'route': pulse['route'], 'data': pulse['data'], 'relaunch': pulse['relaunch']}

        elif type(pulse) == Pulse:
            self.log.debug("Pulse from {} [{}]".format(self.pids[pulse.pid]['name'], pulse.pid), path='@bd.watchdog')
            self.pids[pulse.pid]['pulse'] = pulse
        elif type(pulse) == str:
            if pulse == 'KILLALL':
                pids = self.pids.keys()
                self.log.critical('KILLING ALL PIDS: {}'.format(pids))
                main_pid = None
                for pid in pids:
                    if self.pids[pid]['name'] == 'BotDriver Main':
                        main_pid = pid
                        continue
                    self.kill_pid(pid)
                self.kill_pid(main_pid)
                return False
        return True


    def get_process_usage(self, pid):
        p = psutil.Process(pid)
        cpu_percents = []
        for i in range(0, 5):
            cpu_percents.append(p.cpu_percent(interval=.1))
        cpu_avg  = float(sum(cpu_percents))/len(cpu_percents)
        data = {
            "pid": pid,
            "memory_percent": p.memory_percent(),
            "cpu_percent": cpu_avg
        }

        threads = p.threads()
        for pid_threads in threads:
            if pid_threads[0] != pid:
                continue
            else:
                data['threads'] = pid_threads[1:]

        return data

    def send_pulse_loop(self, pid=None, delay=10000):
        if not pid:
            pid = os.getpid()
        while (1):
            sleep(delay/1000)
            self.q.put(Pulse(pid),0)

    def send_pulse(self, pid=None, tmp_grace=None):
        if not pid:
            pid = os.getpid()
        grace = self.grace
        if tmp_grace:
            grace = tmp_grace
        self.q.put(Pulse(pid, grace=grace),0)

    def __track_process__(self, pid=None, name=None, route=None, grace=False, data={}, relaunch=False):
        if not pid:
            pid = os.getpid()
        if not name:
            name = "Process {}".format(pid)

        if grace == False:
            grace = self.grace

        self.q.put({
            'name': name,
            'route': route,
            'data': data,
            'relaunch': relaunch,
            'pulse': Pulse(pid, grace)
        },0)


class Pulse (object):
    def __init__(self, pid, grace=30):
        self.pid = pid
        self.time = datetime.utcnow()
        self.grace = grace
        self.flag = False

    def is_expired(self, dt):
        if self.grace==None:
            return False
        return dt > self.time + timedelta(seconds=self.grace)

