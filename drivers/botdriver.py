import os
import uuid
import multiprocessing
from multiprocessing.connection import Listener
#https://stackoverflow.com/questions/6920858/interprocess-communication-in-python#answer-6921402
from bn2.utils.watchdog import Heartbeat
from bn2.utils.msgqueue \
    import PriorityQueue, create_local_task_message, INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG
import traceback
from datetime import datetime, timedelta
import threading
import bn2.utils.logger
from twisted.internet import task


def dead_process_cb():
    print('dead process')

def dead_route_process_cb(route, data):
    pass

class BotDriver (object):
    INBOX_PRIORITIES = 5
    OUTBOX_PRIORITIES = 2
    start_up_route_funcs = []
    route_mappings = {}
    queued_routes = []
    inbox_msg_id=0
    RUNNING = False
    check_funcs = []
    HEARTBEAT_PULSE_INTERVAL = 1000 * 10
    outbox_cb = None

    is_timer_init = False

    def __init__(self, config):
        self.uuid = BotDriver.create_bot_uuid()

        self.__bd_route_mappings = {
            '@bd.watchdog.launch': self.bd_watchdog_launch,
            '@bd.process.died': self.bd_process_died,
            '@bd.comms.launch': self.bd_comms_launch,
            '@bd.process.kill': self.bd_process_kill,
            '@bd.echo': self.bd_echo
        }
        self.add_route_mappings(self.__bd_route_mappings)

        self.add_check_func(self.__pulse_watchdog__, self.HEARTBEAT_PULSE_INTERVAL)

        wd_msg = create_local_task_message('@bd.watchdog.launch', {}, {'type': 'process', 'daemon': True})
        comms_msg = create_local_task_message('@bd.comms.launch',{}, {'type': 'process'})

        self.add_start_up_route(wd_msg, INBOX_SYS_MSG)
        self.add_start_up_route(comms_msg, INBOX_SYS_MSG)


        self.log = bn2.utils.logger.Logger('MainDriver', color=True)


    @staticmethod
    def create_bot_uuid():
        return str(uuid.uuid1())


    def init_start_up_routes (self):
        for route_func in self.start_up_route_funcs:
            route_func()

    def add_start_up_route(self, msg, priority=INBOX_TASK1_MSG):
        task_msg = lambda: self.inbox.put(msg, priority)
        self.start_up_route_funcs.append(task_msg)

    def add_check_func(self, func, delay):
        self.check_funcs.append([func, delay])


    def add_route_mapping(self, route, func):
        self.route_mappings[route] = func

    def add_route_mappings(self, maps):
        self.route_mappings.update(maps)

    def run_route(self):
        pass


    def route(self, route):
        """
        to make this work work on driver page we can add _register to check for
        functions on module pages
        """
        def decorator(f):
            f._ROUTE_ = route
            self.add_route_mapping(route, f)
            return f
        return decorator

    def register(self, *objects):
        self._register(objects[0])
        self.model_tag = objects[0].model_tag
        #for obj in objects:
        #    self._register(obj)

    def _register(self, obj):
        for name in dir(obj):
            if name.startswith("_"):
                continue
            attr = getattr(obj, name)
            if callable(attr) and hasattr(attr, "_ROUTE_"):
                self.add_route_mapping(attr._ROUTE_, attr)

    def __pulse_watchdog__(self):
        self.heartbeat.send_pulse(os.getpid())

    def run_route_thread(self):
        pass

    def bd_watchdog_launch(self, data, route_meta):
        self.heartbeat.watchdog()

    def bd_comms_launch(self, data, route_meta):
        self.run_comms()

    def bd_process_kill(self, data, route_meta):
        self.log.debug('Killing process {}'.format(data['pid']))
        os.system('kill -9 {}'.format(data['pid']))

    def bd_process_died(self, data, route_meta):
        print("DIED: {}".format(data))

    def bd_echo(self, data, route_meta):
        self.log.critical("ECHO: {}".format(data))

    def report_issue(self, msg, path, *, ERROR_LVL=False, WARNING_LVL=False, CRITICAL_LVL=False, KILL=False):
        if not ERROR_LVL and not CRITICAL_LVL:
            WARNING_LVL = True
            self.log.warning(msg, path=path)

        if ERROR_LVL:
            self.log.error(msg, path=path)

        elif CRITICAL_LVL:
            self.log.critical(msg, path)

        if KILL:
            raise NotImplemented

    def create_route_process(self, route, data, route_meta, daemon=False):
        process = multiprocessing.Process(
            target=self.router,
            args=(route, data, route_meta)
        )
        process.daemon = daemon
        process.start()
        return process

    def run_router_msg(self, msg):
        route_meta = {}
        if 'route_meta' in msg.keys():
            route_meta = msg['route_meta']

            meta_keys = route_meta.keys()
            if not 'type' in meta_keys:
                route_meta['type'] = 'default'

        route_meta['route'] = msg['route']

        if route_meta['type'] == 'process':
            daemon = False
            if 'daemon' in route_meta.keys():
                daemon = route_meta['daemon']
            self.create_route_process(msg['route'], msg['data'],route_meta, daemon=daemon)
            return


        resp = self.router(msg['route'], msg['data'], route_meta)

    def router(self, route, data, route_meta):
        error = True
        msg = 'Uncaught Error!'
        try:
            resp = self.route_mappings[route](data, route_meta)

        except KeyError as e:
            if route == str(e):
                msg = 'Route "{}" does not exist'.format(route)
                print(msg)
                #self.report_error('bd.router.err', msg)
            else:
                msg = "KeyError issue in code: '{}'".format(e)
                print(traceback.print_exc())
                #self.report_error('bd.router.unk_err', msg)

        except Exception as e:
            msg = str(e)
            print(traceback.print_exc())
            #self.report_error('bd.router.unk_err', msg)

        else:
            error = False
            msg = None

        return error, msg



    def run_comms (self):
        raise NotImplemented

    def init_mailbox(self, manager):
        self.manager = manager
        self.inbox = PriorityQueue(self.INBOX_PRIORITIES, manager=manager)
        self.outbox = PriorityQueue(self.OUTBOX_PRIORITIES, manager=manager)
        self.heartbeat_inbox = PriorityQueue(1, manager=manager)
        self.taskrunner_inbox = PriorityQueue(1, manager=manager)
        self.queued_routes = PriorityQueue(2)

    def init_outbox_callback(self):
        if self.outbox_cb:
            self.outbox_cb.stop()

        self.outbox_cb = task.LoopingCall(self.check_outbox)
        ob = self.outbox_cb.start(.01, now=False)

    def check_queued_routes(self):
        msg, priority = self.queued_routes.get(get_priority=True)
        if msg:
            self.log.info("<{}:RUNNING> [{}]".format(msg['route_meta']['msg_id'], msg['route']), path='bd.routes.queue')

            self.run_router_msg(msg)

    def check_outbox(self):
        msg = self.outbox.get()
        if msg:
            if self.factory:
                self.factory.send_it(msg)
            else:
                self.issue_error('Factory has not been set', 'bd.outbox', CRITICAL_LVL=True)
                pass

    def check_inbox(self):
        msg, p = self.inbox.get(get_priority=True)
        if msg:
            self.inbox_msg_id+=1
            msg['route_meta']['msg_id'] = self.inbox_msg_id
            now = datetime.utcnow().strftime('%H:%M:%S.%f')
            #self.log.debug("<msg#{}:M> {}".format(self.inbox_msg_id, msg['route_meta']), path='bd.inbox')
            #self.log.debug("<msg#{}:D> {}".format(self.inbox_msg_id, msg['data']), path='bd.inbox')
            if (type(msg) != type({})):
                self.log.critical("inbox_msg_id {} is corrupted: {}".format(self.inbox_msg_id), msg)
                return


            #if msg is interbot comms
                #self.run_router_msg(msg)
            #else:

            if msg['route_meta']['type']=='process':
                self.log.info("<{}:P> [{}]".format(self.inbox_msg_id, msg['route']), path='bd.inbox')


                self.run_router_msg(msg)
                return

            route_priority = 1
            if '@bd' or '@md' or '@sd' in msg['route']:
                route_priority = 0


            self.queued_routes.put(msg, route_priority)

        #msg = get_msgs ie
        #if msg ==
        pass

    def thread_func_loop(self, func):
        while self.RUNNING:
            func()

    def loop(self):
        def get_timer_ring(delay):
            delay = timedelta(milliseconds=delay)
            timer_ring = datetime.utcnow() + delay
            return timer_ring

        init_loop = False
        if not self.is_timer_init:
            init_loop = True
            self.is_timer_init = True

        while self.RUNNING:
            for i, funcd in enumerate(self.check_funcs):
                timer_ring = get_timer_ring(funcd[1])
                func = funcd[0]
                if init_loop:
                    timer_ring = get_timer_ring(funcd[1])
                    self.check_funcs[i].append(timer_ring)

                if self.check_funcs[i][2] <= datetime.utcnow():
                    func()
                    self.check_funcs[i][2] = get_timer_ring(funcd[1])

            init_loop = False


    def create_thread(self, func, args=[], daemon=False, start=False):
        thread = threading.Thread(target=func, args=(tuple(args),))
        thread.daemon = daemon
        if start:
            thread.start()
        return thread

    def create_loop_thread(self, func, name):
        return threading.Thread(
            target=self.thread_func_loop,
                args=(func,),
            name=name
        )

    @staticmethod
    def get_model_tag_from_route(route):
        model_id = route[route.find('@')+1:].split('.')[0]
        return model_id

    def start(self):
        pid = os.getpid()
        self.RUNNING = True
        with multiprocessing.Manager() as manager:
            self.log.info('MAIN PID: {}'.format(pid))
            self.init_mailbox(manager)
            self.heartbeat = Heartbeat(
                self.heartbeat_inbox,
                self.inbox,
                grace=15,
                dead_cb=dead_process_cb,
                parent_pid=pid
            )
            self.heartbeat.set_logger(self.log)

            check_inbox_thread = self.create_loop_thread(self.check_inbox, 'Inbox Checker')
            #checks inbox and moves either to queued_routes or sends message
            queued_routes_thread = self.create_loop_thread(self.check_queued_routes, 'Route Runner')

            queued_routes_thread.start()
            check_inbox_thread.start()

            self.init_start_up_routes()
            self.heartbeat.__track_process__(pid, name='BotDriver Main')


            self.loop()

            check_inbox_thread.join()
            queued_routes_thread.join()




'''
1) driver (handles networking)
2) endpoints (handles running tasks)

driver might have two threads, 1 for networking
another other for interprocess socket communication,
and another one for running routes

-----MAYBE HAVE 4 THREADS
------------- (checks inbox[adds to router_queue]  & interbot comms [bypasses routing],
------------- routing,
------------- other low-level tasks check_timer, check_slave_pulse
------------- sockets

-MAKE A MPPRIORITYQUEUE FOR QUEUD ROUTES FOR USE IN INTERTHREAD COMMS
-3 priorities
--- 1) bot network messaging (sending messages to other  bots)
--- 2) maybe? low-level tasks (confirming slave join, task completed, etc)
--- 3) maybe? all other tasks (maybe join 2 & 3 together)

endpoints has 1 thread for reading from socket, and other threads for running tasks

inter-thread commuincation is using 2 array like obj for in and out


any changes to driver state happens in driver process and endpoint process will hagve to be recreated

find a way to have endpoints variables updated from driver without relainching process

create a function that assigns variable to instance so when endpoint thread wants to change value to calls function that handles changing the state for other processes

ie def set_self_var(self, var, val):
    with getLock():
        setattr(var, val)
        self.state_changed_flag = True #maybe relaunch process after?

'''
