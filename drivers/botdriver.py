#where it started...
import __main__
import os
import sys
import secrets
import uuid
import multiprocessing
import json
from bn2.utils.watchdog import Heartbeat, StoppableThread
from bn2.utils.msgqueue \
    import PriorityQueue, create_local_task_message, create_local_route_meta, safe_copy_route_meta, INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG
import traceback
from datetime import datetime, timedelta
import threading
import bn2.utils.logger
from twisted.internet import task
import setproctitle

import jwt
from Crypto.PublicKey import RSA
from base64 import b64encode, b64decode
from functools import wraps
import hashlib



ASYNC_ROUTE = "AR"
SYNC_ROUTE = "SR"

SYNC_PRIORITY = 0
ASYNC_PRIORITY = 1

GLOBAL_JOB_ROUTE_SIGN = '$'
GLOBAL_TASK_ROUTE_SIGN = '+'



def dead_route_process_cb(route, data):
    pass

def route(*args, **kwargs):
    #args[0] = self
    #args[1] = route
    print(args)
    return args[0].route(args[1], **kwargs)

def route_guard(*args, **kwargs):
    return BotDriver.route_guard(*args, **kwargs)


class BotDriver (object):
    token = None
    model_tag = 'bd'
    INBOX_PRIORITIES = 5
    OUTBOX_PRIORITIES = 2
    start_up_route_funcs = []
    queued_routes = []
    inbox_msg_id=0
    RUNNING = False
    check_funcs = []
    HEARTBEAT_PULSE_INTERVAL = 1000 * 10
    outbox_cb = None

    bot_configs_path = None

    is_timer_init = False

    def __init__(self, config):
        self.config = config

        self.route_levels = []

        self.driver_events = {}
        self.route_mappings = {}

        if len(sys.argv)>1:
            if not os.path.exists(sys.argv[1]):
                raise ValueError("Bot configs file '{}' doesn't exist".format(sys.argv[1]))

            print("Setting bot configs from file: {}".format(sys.argv[1]))
            self.bot_configs_path = sys.argv[1]


        self._init_bot_configs()

        if not self.get_bot_config('uuid', throw=False): #if uuid is already set by slave configs
            self.uuid = BotDriver.create_bot_uuid()



        self._SLAVE_DIR_ = os.path.dirname(os.path.realpath(__main__.__file__))
        self.local_route_secret = self.create_secret(16)

        self.add_route_mapping('@bd.die', self.bd_die, is_async=True)
        self.add_route_mapping('@bd.configs.export', self.bd_configs_export, is_async=False)
        self.add_route_mapping('@bd.watchdog.launch', self.bd_watchdog_launch, is_async=True, level="LOCAL")
        self.add_route_mapping('@bd.process.died', self.bd_process_died, is_async=True) #if you want to set to local pass secret to watchdog
        self.add_route_mapping('@bd.comms.launch', self.bd_comms_launch, is_async=True, level="LOCAL", only_process=True)
        self.add_route_mapping('@bd.process.kill', self.bd_process_kill, is_async=True, level="LOCAL")
        self.add_route_mapping('@bd.usage', self.bd_usage, is_async=True)
        self.add_route_mapping('@bd.tag.add', self.bd_tag_add, is_async=True)


        self.add_route_mapping('@bd.echo', self.bd_echo, is_async=True)


        self.add_check_func(self.check_delayed_inbox, 1500)
        #self.add_check_func(self.__pulse_watchdog__, self.HEARTBEAT_PULSE_INTERVAL)



        comms_msg = self.create_local_task_message('@bd.comms.launch',{}, is_process=True)

        self.add_start_up_route(comms_msg, INBOX_SYS_MSG)


        dir_ = self.get_bot_config('log_out_dir', throw=True)


        logger_id = '{}@{}'.format(self.model_tag, self.uuid)

        self.log_out_path = None

        if dir_ or self.get_bot_config('enable_log_upload'):
            if not dir_:
                dir_ = os.getcwd()
            self.log_out_path = os.path.join(dir_, logger_id)
            self.log = bn2.utils.logger.Logger('MainDriver', color=True, to_file=self.log_out_path)

        else:
            self.log_out_path = os.path.join(os.getcwd(), logger_id)
            self.log = bn2.utils.logger.Logger('MainDriver', color=True)



    @staticmethod
    def create_bot_uuid(short=False):
        if short:
            return str(uuid.uuid4())[-12:]
        return str(uuid.uuid1())


    @staticmethod
    def create_secret(num=32):
        return secrets.token_hex(num)

    def init_start_up_routes (self):
        for msg, priority in self.start_up_route_funcs:
            self.add_local_msg(msg, priority)
        self.start_up_route_funcs = []

    def add_start_up_route(self, msg:dict, priority:int=INBOX_SYS_MSG):
        self.start_up_route_funcs.append([msg, priority])

    def add_check_func(self, func, delay:int):
        self.check_funcs.append([func, delay])

    def add_route_mapping(self, route, func, *, is_async=False, only_global_job=False, only_global_task=False, only_process=False, level=None):
        if only_global_job and only_global_task:
            raise ValueError("route '{}' cannot have both global job and task sign")

        if only_global_job:
            if route[-1]!=GLOBAL_JOB_ROUTE_SIGN:
                raise TypeError("route '{}' does not have correct global job sign '{}'".format(route, GLOBAL_JOB_ROUTE_SIGN))

        if only_global_task:
            if route[-1]!=GLOBAL_TASK_ROUTE_SIGN:
                raise TypeError("route '{}' does not have correct global task sign '{}'".format(route, GLOBAL_TASK_ROUTE_SIGN))

        self.route_mappings[route] = [func, is_async, only_process, level]

    def add_route_mappings(self, maps):
        self.route_mappings.update(maps)

    def does_route_exist(self, route):
        try:
            self.route_mappings[route]
        except KeyError:
            return False
        else:
            return True

    def get_route_func(self, route, throw=True):
        try:
            return self.route_mappings[route][0]
        except KeyError:
            if throw:
                raise KeyError('Route "{}" does not exist'.format(route))


    def is_route_only_process(self, route, throw=True):
        try:
            return self.route_mappings[route][2]
        except KeyError:
            if throw:
                raise KeyError('Route "{}" does not exist'.format(route))
            return False

    def is_route_async(self, route, throw=True):
        try:
            return self.route_mappings[route][1]
        except KeyError:
            if throw:
                raise KeyError('Route "{}" does not exist'.format(route))
            return False

    def is_route_global(self, route):
        return route[-1]=='+' or route[-1]=='$'




    def get_level_from_route(self, route, throw=True):
        try:
            return self.route_mappings[route][3]
        except KeyError:
            if throw:
                raise KeyError('Route "{}" does not exist'.format(route))

    def get_route_level_bitmask(self, level):
        try:
            idx = self.route_levels.index(level)

        except ValueError:
            raise KeyError("route level '{}' does not exist".format(level))

        else:
            return (idx+1**2)

    def add_local_msg(self, msg, priority=None, delay=None, *args, **kwargs):
        if delay:
            delay_secs = datetime.utcnow().timestamp()+delay
            self.inbox_delayed.put([msg, priority, delay_secs], 0)
            return

        self.inbox.put(msg, priority=priority)

    def has_route_level(self, perms, *levels, include_op=False):
        if not self.route_levels:
            raise Exception("route levels has not been set by master")

        levels = list(levels)
        if include_op:
            #remove this
            raise NotImplemented

        for level in levels:
            if level == '*':
                return True

            mask = self.get_route_level_bitmask(level)
            if (perms & mask) == mask:
                return True

        return False


    def get_rsa_key_from_str(self, rsa_str):
        key = RSA.importKey(rsa_str)
        return key.exportKey('PEM')


    def generate_rsa_key_pair(self, bits=1024, as_str=False):
        key = RSA.generate(bits)
        pub = key.publickey().exportKey('PEM')
        priv = key.exportKey('PEM')
        if as_str:
            return pub.decode('utf-8'), priv.decode('utf-8')
        return pub, priv


    def create_hash_from_source(self):
        BLOCKSIZE = 65536
        hasher = hashlib.md5()
        with open(__file__, 'rb') as f:
            buf = f.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(BLOCKSIZE)

        print("hash of {}\n\n{}".format(__file__, hasher.hexdigest()))
        return hasher.hexdigest()

    def encode_payload(self, token, priv_key, return_str=True):

        t = jwt.encode(token, priv_key, algorithm='RS256')
        if return_str:
            t = t.decode()
        return t

    def decode_payload(self, token, pub_key=None):
        if not pub_key:
            if not self.bn2_public_key:
                raise ValueError("BN2 public key is not set")
            pub_key = self.bn2_public_key


        if type(token) == str:
            token = token.encode()
            #token = b64decode(token)

        return jwt.decode(token, pub_key, algorithms=['RS256'])

    def create_local_task_message(self, route, data, *, route_meta=None, pass_secret=False, **kwargs):
        secret = None
        token = self.token
        if self.get_level_from_route(route, throw=False) == "LOCAL":
            pass_secret = True

        if pass_secret:
            secret = self.local_route_secret
            token = None


        msg = create_local_task_message(
            route, data, route_meta=route_meta,
            local_route_secret=secret, token=token,
            **kwargs
        )
        return msg

    def add_driver_event(self, event, func):
        setattr(self, 'DRIVER_EVENT_'+event, func)
        #self.driver_events[event] = func

    def remove_driver_event(self, event):
        raise NotImplemented
        #del self.driver_events[event]
        delattr(self, 'DRIVER_EVENT_'+event)



    def check_route_level(self, level, meta):
        authenticated = False
        err_msg =""

        if level=='LOCAL':
            if self.local_route_secret == meta.get('local_route_secret', None):
                authenticated = True

        elif meta['token']:
            token = None
            try:
                token = self.decode_payload(meta['token'])
                if token:
                    authenticated = self.has_route_level(token['bitmask'], level)

            except Exception as e:
                self.log.error("check_route_level error: {}".format(e), path='def check_route_level')
                print(traceback.print_exc())
                authenticated = False


        return authenticated

    def route_guard(self, level=""):
        def decorator(f):
            self.route_mappings[f._ROUTE_][3] = level
            return f
        return decorator

    def route(self, route, **kwargs):
        """
        to make this work work on driver page we can add _register to check for
        functions on module pages
        """
        def decorator(f):
            f._ROUTE_ = route
            self.add_route_mapping(route, f, **kwargs)
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

    def get_model_tags_route(self, mt):
        routes = []
        def find(route, model):
            if model in route:
                routes.append(route)

        [find(route, mt) for route in self.route_mappings.keys()]
        return routes

    def _init_bot_configs(self):

        self.add_bot_config(
            'master_ip',
            'Master IP',
            'str',
            default='0.0.0.0'
        )

        self.add_bot_config(
            'master_port',
            'Master Port',
            'number',
            default=5600
        )

        self.add_bot_config(
            'bn2_public_key',
            'BN2 Public RSA Key',
            'text',
            desc='DO NOT MODIFY',
        )



        self.add_bot_config(
            'queued_async_thread_cnt',
            'Exclusive Queued ASYNC thread count',
            'number',
            desc='Number of threads to exclusively handle async queued routes',
            default=2
        )

        self.add_bot_config(
            'bn2_lib_dir',
            'Dir for BN2 library',
            'str'
        )


        self.add_bot_config(
            'watchdog_usage_send_interval',
            'Interval (in seconds) between system usage send',
            'number',
            desc='How often usage info gets sent from watchdog.  0 to disable.',
            default=0
        )


        self.add_bot_config(
            'log_out_dir',
            'Dir for log output',
            'path',
            default="%%"
        )

        self.load_bot_config_from_file()




    def export_bot_config_to_file(self, fn):
        with open(fn, 'w') as f:
            f.write('[\n')
            cfgs = []
            for key in self.bot_config_map.keys():

                add_comma = False
                if len(cfgs):
                    add_comma = True

                out = '\t'+json.dumps({'key': key, 'value': self.bot_config_map[key]['value']})
                if add_comma:
                    out = ',\n'+out
                cfgs.append(out)

            f.writelines(cfgs)
            f.write('\n]\n')

    def load_bot_config_from_file(self, fn=None):
        if not fn:
            fn = self.bot_configs_path
        if fn:
            with open(fn, 'r') as f:
                cfg_str = f.read()
                self.parse_bot_config(json.loads(cfg_str))



    def parse_bot_config(self, configs):

        for cfg in configs:
            if cfg['key'] in self.bot_config_map.keys():
                self.set_bot_config(cfg['key'], cfg['value'])


    def set_bot_configs(self, **kwargs):
        for key, value in kwargs.items():
            self.set_bot_config(key, value)

    def set_bot_config(self, key, value):
        if self.bot_config_map[key]['type'] == 'number':
            if type(value)  != float or type(value) != int:

                value = float(value)
                val_int = int(value)
                if val_int == value:
                    #this helps remove trailing float 0 when export configs
                    value = val_int


        self.bot_config_map[key]['value'] = value


    def get_bot_configs(self):
        return [*self.bot_config_map.values()]

    def get_bot_config(self, key, throw=False, value=True):
        cfg = self.bot_config_map.get(key, None)
        if not cfg:
            if throw:
                raise Exception("Bot config does not have key '{}'".format(key))
            return None


        if value:
            val = self.bot_config_map[key]['value']
            if not val:
                return None
            if self.bot_config_map[key]['type'] == 'path':
                if '%%' in val:
                    folder = os.path.dirname(os.path.realpath(__main__.__file__))
                    val = val.replace("%%", folder)
                val = os.path.expanduser(val)
            return val

        return self.bot_config_map[key]



    def add_bot_config(self, key, label, _type, desc=None, default=None, grouping=None):
        cfg = {'type': _type, 'label':label, 'key': key, 'desc': desc, 'default':default, 'value': default}
        if grouping:
            cfg['grouping'] = grouping
        try:
            getattr(self, 'bot_config_map')
        except:
            self.bot_config_map = {}
        finally:
            self.bot_config_map[key] = cfg

    def remove_bot_config(self, key):
        del self.bot_config_map[key]

    def set_process_name(self, name, raw=False):
        pname = "bn2:"+name+'@'+self.uuid
        if raw:
            pname = name

        setproctitle.setproctitle(pname)


    def die(self, msg=None):
        if msg:
            self.log.critical("Killing Bot! "+msg)
        #for thread in self.active_threads:
        #    thread.join()
        self.heartbeat.KILLALL()
        self.RUNNING = False
        #sys.exit(0)


    def bd_configs_export(self, data, route_meta):
        path = data.get('path', None)
        if not path:
            path = self.bot_configs_path
        self.export_bot_config_to_file(path)

    def bd_die(self, data, route_meta):
        msg = None
        if 'msg' in data.keys():
            msg = data['msg']
        self.die(msg)


    def bd_watchdog_launch(self, data, route_meta):
        self.heartbeat.watchdog()

    def bd_comms_launch(self, data, route_meta):
        #PROCESS ONLY ROUTE

        #maybe put and if statement here to check to launc comms
        #could help when process died and it is being relaunched but might not need it (ie failed to many times)

        self.set_process_name("comms")
        self.run_comms()

    def bd_process_kill(self, data, route_meta):
        pids = data.get('pids', [])
        pids.append(data.get('pid', None))

        for pid in pids:
            if pid:
                self.log.warning('Trying to kill pid [{}]'.format(pid))
                os.system('kill -9 {}'.format(pid))

    def bd_process_died(self, data, route_meta):
        self.log.debug("Proccess died - route: {} Relaunch:{}".format(data['route'], data['relaunch']))
        if data['relaunch']:
            msg = self.create_local_task_message(data['route'], data['data'], is_process=True)
            self.inbox.put(msg, INBOX_SYS_CRITICAL_MSG)

    def bd_echo(self, data, route_meta):
        self.log.critical("ECHOOOOING: {}".format(data))

    def bd_tag_add(self, data, route_meta):
        self.tag_inbox.put(data)

    def bd_usage(self, data, route_meta):
        self.handle_bot_usage(data)

    def handle_bot_usage(self, usage):
        raise NotImplemented
        #SlaveDriver and MasterDriver overwrite this method
        pass

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


        err = self.router(msg['route'], msg['data'], route_meta)



    def sleep(self, delay_ms):
        self.thread_event.wait(delay_ms/1000)

    def router(self, route, data, route_meta):
        error = 'Uncaught Error!'
        resp = {}
        if not route_meta.keys():
            route_meta = create_local_route_meta()

        try:
            route_level = self.get_level_from_route(route)
            decoded_token = None
            if route_level:
                authenticated = False
                if route_level == 'LOCAL':
                    if self.local_route_secret == route_meta.get('local_route_secret', None):
                        authenticated = True

                else:
                    decoded_token = self.decode_payload(route_meta['token'])
                    if decoded_token:
                        authenticated = self.has_route_level(decoded_token['bitmask'], route_level)


                if not authenticated:
                    raise ValueError("Origin {} Route {}  lvl '{}' failed authentication".format(route_meta['origin'], route, route_level))


            func_args = [data, route_meta]
            if decoded_token:
                func_args.append(decoded_token)

            resp = self.get_route_func(route)(*func_args)

        except KeyError as e:
            if route == str(e):
                error = 'Route "{}" does not exist'.format(route)
                #self.report_error('bd.router.err', msg)
            else:
                error = "KeyError issue in code: '{}'".format(e)
                print(traceback.print_exc())
                #self.report_error('bd.router.unk_err', msg)

        except Exception as e:
            error = str(e)
            print(traceback.print_exc())
            #self.report_error('bd.router.unk_err', msg)

        else:
            error = None

        cb_route = route_meta.get('callback', None)
        if cb_route:
            del route_meta['callback']
            mt = self.get_model_tag_from_route(cb_route)
            self.log.info("Route '{}' returned data: {}".format(route, resp))
            cb_bot_type = self.get_bot_type_from_route(cb_route)

            is_local = False

            is_global = self.is_route_global(cb_route)


            if self.model_tag == mt or cb_bot_type=='BOT' and not is_global:
                #EITHER SAME MODEL OR BOT CALL
                is_local=True

            elif mt == 'md' and self.BOT_TYPE == cb_bot_type:
                #CHECKS IF
                is_local = True


            elif mt == 'sd' and self.BOT_TYPE == cb_bot_type and not is_global:
                is_local = True


            sent = False

            msg = None

            if is_global and self.BOT_TYPE == 'MASTER':
                #this is master
                msg = self.create_slave_task_message(
                    cb_route, resp, 'callback', route_meta=route_meta, job_id=route_meta['job_id']
                )

                self.inbox.put(msg)
                sent = True



            if is_local and not sent and not is_global:
                msg = create_local_task_message(cb_route, resp, route_meta=route_meta)
                self.inbox.put(msg)

            else:
                if self.BOT_TYPE == "SLAVE":
                    if is_global and not sent:
                        #if global slave task
                        self.add_global_task(cb_route, resp, "callback", job_id=route_meta['job_id'])
                        sent = True

                    if cb_bot_type == "MASTER" and not sent:
                        self.send_message_to_master(msg)
                        sent = True

                if not sent and msg:
                    self.send_message_to(route_meta['origin'], msg)


        return error


    def run_comms (self):
        raise NotImplemented

    def init_mailbox(self, manager):
        self.manager = manager
        self.inbox = PriorityQueue(self.INBOX_PRIORITIES, manager=manager)
        self.inbox_delayed = PriorityQueue(1, manager=manager)
        self.outbox = PriorityQueue(self.OUTBOX_PRIORITIES, manager=manager)
        self.heartbeat_inbox = PriorityQueue(1, manager=manager)
        self.tag_inbox = PriorityQueue(1, manager=manager)
        self.queued_routes = PriorityQueue(3)

    def init_outbox_callback(self):
        if self.outbox_cb:
            self.outbox_cb.stop()

        self.outbox_cb = task.LoopingCall(self.check_outbox)
        ob = self.outbox_cb.start(.01, now=False)

    def send_comm_cmd(self, cmd, **kwargs):
        kwargs['comm_cmd'] = cmd
        self.outbox.put(kwargs, OUTBOX_SYS_MSG)

    @staticmethod
    def get_tag_uuid(tag):
        if tag[0] == '#':
            return tag.split('@')[1]
        raise TypeError("str '{}' is not a tag".format(tag))

    def create_tag(self) -> str:
        #in the future when task id is assigned to current self, this func can be created by local tasks
        #using random generater based on time rather

        rnd_uuid = BotDriver.create_bot_uuid(short=True)
        id_ = rnd_uuid
        SET = False

        if self.BOT_TYPE == 'SLAVE':
            if self.RUNNING_TASK_ID:
                id_ +='tid'+str(self.RUNNING_TASK_ID)
                SET = True

        if not SET:
            id_ +='anon'

        return '#{}@{}'.format(id_, self.uuid)

    def send_tag_data(self, tag, data):
        uuid = self.get_tag_uuid(tag)
        out = {
            "tag": tag,
            "data": data
        }
        if uuid == self.uuid:
            self.tag_inbox.put(out)
            return

        msg = create_local_task_message('@bd.tag.add', out)
        self.send_message_to(uuid, msg)


    def get_tag_data(self, tag:str, delay:int,  *, get_all:bool=False, raise_error:bool=False, err_msg=None):
        if self.get_tag_uuid(tag) != self.uuid:
            raise ValueError("Cannot access foreign slave's tag data: "+tag)

        CHECK_DELAY = 500
        poll = int(delay*1000/CHECK_DELAY)
        for i in range(0, poll):
            if i: #delay after first poll
                self.sleep(CHECK_DELAY)

            f_msg = None #first msg
            msg = 1
            msgs = []

            while (msg != None and msg != f_msg):
                msg = self.tag_inbox.get()
                if (msg):
                    if msg['tag'] == tag:
                        msgs.append(msg['data'])
                        if not get_all:
                            return msgs[0]
                    else:
                        if not f_msg: #sets as first message looked at
                           f_msg = msg
                        self.tag_inbox.put(msg, 0) #adds back un needed tag data back to queue
            if msgs:
                return msgs

        if not len(msgs) and raise_error:
            if not err_msg:
                err_msg = 'Tag "{}" was polled until timeout'.format(tag)
            raise Exception(err_msg)

    def check_delayed_inbox(self):
        now = datetime.utcnow().timestamp()
        msgs = self.inbox_delayed.get(get_all=True)
        for  msg, priority, run_time in msgs:
            if now >= run_time:
                self.inbox.put(msg, priority)
                continue
            self.inbox_delayed.put([msg, priority, run_time], 0)

    def check_queued_routes(self, priority=None):
        msg = None

        if priority == None:
            self.check_queued_routes(SYNC_PRIORITY)
            self.check_queued_routes(ASYNC_PRIORITY)
            return

        else:
            msg, priority = self.queued_routes.get(priority=priority, get_priority=True)

        if msg:
            log_path = '@bd.routes.queued >> {}'.format(msg['route'])
            self.log.info("msg_id={}|level={}".format(msg['route_meta']['msg_id'], self.get_level_from_route(msg['route'])), path=log_path)

            is_global_task = False
            if self.BOT_TYPE == 'SLAVE':
                if msg['route'] == 'bd.@sd.task.global.start':
                    is_global_task = True
                    thread_id = threading.current_thread().ident

                    self.global_task_info = {"thread_id": thread_id, "pids":[]}

            self.run_router_msg(msg)

            if is_global_task:
                self.global_task_info = {"thread_id": None, "pids":[]}

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
            now = datetime.utcnow()
            msg['route_meta']['_time_received'] = now
            now = now.strftime('%H:%M:%S.%f')
            #self.log.debug("<msg#{}:M> {}".format(self.inbox_msg_id, msg['route_meta']), path='@bd.inbox')
            #self.log.debug("<msg#{}:D> {}".format(self.inbox_msg_id, msg['data']), path='@bd.inbox')
            if (type(msg) != type({})):
                self.log.critical("inbox_msg_id {} is corrupted: {}".format(self.inbox_msg_id, msg), path="@bd.inbox.check")
                return

            if (not self.does_route_exist(msg['route'])):
                self.log.error('Route "{}" does not exist'.format(msg['route']), path="@bd.inbox.check")
                return



            route_level = self.get_level_from_route(msg['route'])
            if route_level:
                if not self.check_route_level(route_level, msg['route_meta']):

                    self.log.warning("Origin {} Route {}  lvl '{}' failed authentication".format(msg['route_meta']['origin'], msg['route'], route_level), path="@bd.inbox.check")

                    if self.BOT_TYPE == 'MASTER':
                        if route_level == "SLAVE":
                            self.log.warning("Warning slave [{}] for unauthorized access.".format(msg['route_meta']['origin']), path='@bd.inbox.check')
                            self.send_comm_cmd("warn_connection", target_uuid=msg['route_meta']['origin'])

                            if msg['route_meta']['token']:
                                decoded_token = self.decode_payload(msg['route_meta']['token'])

                    return

            if self.is_route_only_process(msg['route'], throw=False):
                msg['route_meta']['type']='process'


            if msg['route_meta'].get('type', None)=='process':
                self.log.info("<{}:P> [{}]".format(self.inbox_msg_id, msg['route']), path='@bd.inbox.check')
                self.run_router_msg(msg)
                return


            if self.BOT_TYPE == 'MASTER': #helps bypass waiting to be queued for message to be passed through
                if msg['route'] == 'bd.@md.slave.message.send':
                    self.run_router_msg(msg)
                    return



            route_priority = SYNC_PRIORITY
            if msg['route'] == '@bd.tag.add':
                self.tag_inbox.put(msg['data'])
                return

            if self.is_route_async(msg['route'], throw=False):
                route_priority = ASYNC_PRIORITY

            self.queued_routes.put(msg, route_priority)


    def thread_func_loop(self, func, args):
        while self.RUNNING:
            if len(args):
                func(*args)
            else:
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
            try:
                for i, funcd in enumerate(self.check_funcs):
                    timer_ring = get_timer_ring(funcd[1])
                    func = funcd[0]
                    if init_loop:
                        timer_ring = get_timer_ring(funcd[1])
                        self.check_funcs[i].append(timer_ring)

                    if self.check_funcs[i][2] <= datetime.utcnow():

                        try:
                            func()
                        except Exception as e:
                            self.log.critical(traceback.print_exc(), path='bd.loop.func')

                        self.check_funcs[i][2] = get_timer_ring(funcd[1])

                init_loop = False
            except KeyboardInterrupt:
                self.die()
                self.RUNNING = False

            except Exception as e:
                self.log.critical(traceback.print_exc(), path='bd.loop')
                raise e
            else:
                pass


    def create_thread(self, func, args=[], daemon=False, start=False):
        thread = None
        if len(args):
            thread = threading.Thread(target=func, args=(tuple(args),))
        else:
            thread = threading.Thread(target=func)

        thread.daemon = daemon
        if start:
            thread.start()
        return thread

    def create_loop_thread(self, func, name, *args):
        return StoppableThread(
            target=self.thread_func_loop,
                args=(func,args,),
            name=name
        )

    def create_slave_task_message(self, route, data, name, job_id=None, job_ok_on_error=False, retry_cnt=None, route_meta=None, _only_task_info=False, timeout=None,**kwargs):

        task_msg = {}
        if route_meta:
            task_msg = create_local_task_message(
                route, data, route_meta=route_meta,
                **kwargs
            )
        else:
            task_msg = self.create_local_task_message(
                route, data, route_meta=route_meta,
                **kwargs
            )

        msg = self.create_local_task_message(
            'bd.@md.slave.task.add',
            task_msg
        )

        msg['data']['name'] = name
        msg['data']['job_ok_on_error'] = job_ok_on_error
        if timeout:
            msg['data']['_timeout'] = timeout

        if job_id:
            msg['data']['job_id'] = job_id


        if retry_cnt:
            msg['data']['retry_cnt'] = retry_cnt

        if _only_task_info:
            return msg['data']
        return msg



    @staticmethod
    def get_bot_type_from_route(route):
        idx = 3
        if route[0] == '@':
            idx=1

        elif route[idx] == '@':
            idx +=1

        _type = route[idx:idx+2]
        if _type == 'md':
            return "MASTER"
        elif _type == 'sd':
            return "SLAVE"
        elif _type == 'bd':
            return "BOT"

        raise ValueError("Unknown bot type '{}'".format(_type))


    @staticmethod
    def get_model_tag_from_route(route):
        if not '@' in route:
            raise ValueError("No @ to specify model: '{}'".format(route))

        route = route.replace('<', '.').replace('>', '.')
        model_id = route[route.find('@')+1:].split('.')[0]
        return model_id

    def create_route_runner_thread(self, priority:str=ASYNC_PRIORITY, start=False)->threading.Thread:
        tname = "Route Runner ({})".format(priority)
        t = self.create_loop_thread(self.check_queued_routes, tname, priority)
        if start:
            t.start()

        return t


    def start(self):
        pid = os.getpid()
        self.RUNNING = True
        self.active_threads = []
        self.global_task_info = {}

        with multiprocessing.Manager() as manager:
            self.set_process_name("?")
            self.log.info('MAIN PID: {} UUID: {}'.format(pid, self.uuid))
            self.init_mailbox(manager)
            self.heartbeat = Heartbeat(
                self.heartbeat_inbox,
                self.inbox,
                grace=15,
                parent_pid=pid,
                usage_send_interval=self.get_bot_config('watchdog_usage_send_interval')
            )
            self.heartbeat.set_logger(self.log)

            self.thread_event = threading.Event()


            watchdog_thread = self.create_thread(self.heartbeat.watchdog, start=True)
            check_inbox_thread = self.create_loop_thread(self.check_inbox, 'Inbox Checker')

            self.active_threads.append(watchdog_thread)
            self.active_threads.append(check_inbox_thread)

            #checks inbox and moves either to queued_routes or sends message
            async_thread_cnt = self.get_bot_config('queued_async_thread_cnt', throw=True)
            self.log.debug("Starting {} extra threads for async check_queued_routes".format(async_thread_cnt))

            for i in range(1+async_thread_cnt):
                priority = SYNC_PRIORITY
                if i>0:
                    priority = ASYNC_PRIORITY

                self.create_route_runner_thread(priority, start=True)

            check_inbox_thread.start()

            self.init_start_up_routes()
            self.heartbeat.__track_process__(pid, name='BotDriver Main', grace=None)
            self.set_process_name("driver")

            self.loop()

            for thread in self.active_threads:
                thread.join()





