from multiprocessing import Queue
import os
from datetime import datetime, timedelta
from bn2.utils.msgqueue import create_local_task_message, INBOX_SYS_MSG, INBOX_SYS_CRITICAL_MSG
import sys
from time import sleep


class Heartbeat (object):
    q = None
    pids = {}
    def __init__(self, queue, outbox, grace=30, dead_cb=None, parent_pid=None):
        self.q = queue
        self.grace = grace
        self.dead_cb = dead_cb
        self.active = False
        self.parent_pid = parent_pid
        self.outbox = outbox

    def KILLALL(self):
        self.q.put('KILLALL',0)

    def kill_pid(self, pid):
        os.system("kill -9 {}".format(pid))
        os.system("sudo kill -9 {}".format(pid))
        os.system('sudo pkill -TERM -P {}'.format(pid))

    def set_logger(self, logger):
        self.log = logger
        self.log.set_path('bd.watchdog')

    def watchdog(self):
        self.log.info("Starting Watchdog pid: {}".format(os.getpid()))
        print('STARTING WATCHDOG')
        while 1:
            self.listen()
            pids = list(self.pids)
            if len(pids):
                if not self.active: #activates heartbeat to begin tracking and exit if no more pids
                    self.active = True

                for pid in pids:
                    pid_info = self.pids[pid]
                    if pid_info['pulse'].is_expired(datetime.utcnow()):
                        if pid_info['pulse'].flag:
                            name = pid_info['name']
                            route = pid_info['meta']['route']
                            data = pid_info['meta']['data']
                            self.log.critical("pid {} {} is inactive - killing process".format(name, pid))
                            if self.parent_pid == pid:
                                self.log.critical("Main pid died so killing everything")
                                for p in pids: #kills all others pids before
                                    if p != pid:
                                        self.kill_pid(p)

                                self.kill_pid(pid)
                                self.kill_pid(os.getpid())
                                sys.exit(1)
                            else:
                                msg = create_local_task_message('@bd.process.kill', {'pid':pid})
                                self.outbox.put(msg,INBOX_SYS_CRITICAL_MSG)

                            del self.pids[pid]

                            if self.dead_cb:
                                dead_msg = create_local_task_message('@bd.process.died', {'route':route,'data':data})
                                self.outbox.put(dead_msg, INBOX_SYS_CRITICAL_MSG)
                                #self.dead_cb(route, data)


                        pid_info['pulse'].flag = True
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
            self.pids[pid]['meta'] = {'route': pulse['route'], 'data': pulse['data']}

        elif type(pulse) == Pulse:
            self.log.debug("Pulse from {} [{}]".format(self.pids[pulse.pid]['name'], pulse.pid))
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

    def __track_process__(self, pid=None, name=None, route=None, data={}):
        if not pid:
            pid = os.getpid()
        if not name:
            name = "Process {}".format(pid)

        self.q.put({
            'name': name,
            'route': route,
            'data': data,
            'pulse': Pulse(pid, self.grace)
        },0)


class Pulse (object):
    def __init__(self, pid, grace=30):
        self.pid = pid
        self.time = datetime.utcnow()
        self.grace = grace
        self.flag = False

    def is_expired(self, dt):
        return dt > self.time + timedelta(seconds=self.grace)

