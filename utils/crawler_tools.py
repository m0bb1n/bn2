from bn2.drivers.slavedriver import SlaveDriver
from selenium.webdriver.common.utils import keys_to_typing
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from functools import reduce
import operator

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains

import json
import random

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem, Popularity

import zipfile
import os.path

import numpy as np
from random import randint


def generate_odds(by):
    return randint(0, by*2) % by == 0


def grecaptcha_obj_get_var_from_path(obj, path):
    return reduce(operator.getitem, path, obj)

def grecaptcha_obj_find_var_path(obj, *, find='callback', p=''):
    for k in obj.keys():

        cpath = p
        cpath+='.'
        if cpath=='.':
            cpath = k
        else:
            cpath+=k

        if find == 'callback':
            if k == 'callback' and obj[k] == '__function__':
                return cpath
        else:
            if k == find:
                return cpath

        if type(obj[k]) == dict:
            path = grecaptcha_obj_find_var_path(obj[k], p=cpath, find=find)
            if path:
                return path


def submit_grecaptcha(wd, response, callback_func=None):
    function_obj = 'window.censorStringify=function(n,r=-1,i=!0){r+=1;var f={};for(var t in n)if(n.hasOwnProperty(t)){var o=n[t];if("object"==typeof o){if(r>3)return;o=censorStringify(n[t],r=r,i=!1)}else"function"==typeof o&&(o="__function__");f[t]=o}return i?JSON.stringify(f):f};'
    #recaptcha_extract.js minified ^
    #wd.execute_script(function_obj)

    #obj_path = "___grecaptcha_cfg.clients[0]"

    #script = 'return censorStringify({})'.format(obj_path)

    #grecaptcha = wd.execute_script(script)

    #callback_func_path = obj_path +'.'+grecaptcha_obj_find_var_path(grecaptcha)
    #sk_key = grecaptcha_obj_find_var_path(grecaptcha, find='sitekey')
    #sk_key = sk_key.split('.')
    #sk = grecaptcha_obj_get_var_from_path(grecaptcha, sk_key)

    if callback_func:
        wd.execute_script("{}('{}')".format(callback_func, response))
    else:
        wd.execute_script("document.getElementById('g-recaptcha-response').style.display = 'block';")
        elm = wd.find_element_by_xpath('//*[@id="g-recaptcha-response"]')
        actions = HumanActions(wd)
        actions.human_move_to_element_from(elm, elm).click()
        actions.perform()
        elm.send_keys(response)



def wait_for_url_change(browser, start_url, delay=30, delay_func=None):
        CHECK_DELAY = 1000
        poll = int(delay*1000/CHECK_DELAY)
        for i in range(0, poll):
            if i: #delay after first poll
                delay_func(CHECK_DELAY)
            if start_url != browser.current_url:
                return True
        return False

def wait_for_element(browser, condition, by, value, delay, throw=True):
    try:
        return WebDriverWait(browser, delay)\
            .until(condition((by, value)))

    except TimeoutException:
        if throw:
            raise TimeoutException("condition {} did not find value '{}' by '{}'".format(condition, value, by))


def wait_for_element_visible(browser, by, value, delay, throw=True):
    return wait_for_element(browser, EC.visibility_of_element_located, by, value, delay, throw=throw)


def wait_for_element_clickable(browser, by, value, delay, throw=True):
    return wait_for_element(browser, EC.element_to_be_clickable, by, value, delay, throw=throw)


class RandomUserAgent(UserAgent):
    def __init__(self, limit, file_path=None, is_zip=True, *args, **kwargs):
        self.user_agents = []
        self.file_path = file_path

        for attribute, values in self.ATTRIBUTES_MAP.items():
            setattr(self, attribute, kwargs.get(attribute, [v.lower() for v in values]))

        for user_agent in self.load_user_agents(file_path=file_path, is_zip=is_zip):
            if limit is not None and len(self.user_agents) >= limit:
                break

            if self.hardware_types and user_agent['hardware_type'].lower() not in self.hardware_types:
                continue
            if self.software_types and user_agent['software_type'].lower() not in self.software_types:
                continue
            if self.software_names and user_agent['software_name'].lower() not in self.software_names:
                continue
            if self.software_engines and user_agent['software_engine'].lower() not in self.software_engines:
                continue
            if self.operating_systems and user_agent['operating_system'].lower() not in self.operating_systems:
                continue
            if self.popularity and user_agent['popularity'].lower() not in self.popularity:
                continue
            self.user_agents.append(user_agent)

    def load_user_agents(self, file_path=None, is_zip=True):
        if not file_path:
            if not self.file_path:
                raise ValueError("File path needs to be provided")
            file_path = self.file_path


        if is_zip:
            with zipfile.ZipFile(file_path) as zipped_user_agents:
                with zipped_user_agents.open('user_agents.jl') as user_agents:
                    for user_agent in user_agents:

                        if hasattr(user_agent, 'decode'):
                            user_agent = user_agent.decode()
                        if len(user_agent)<2:
                            continue

                        yield json.loads(user_agent)
        else:
            with open(file_path, 'r') as user_agents:
                for user_agent in user_agents:
                    if hasattr(user_agent, 'decode'):
                        user_agent = user_agent.decode()
                    yield json.loads(user_agent)



class HumanActions (ActionChains):
    def set_sleep_func(self, func):
        self.sleep_func = func

    def sleep(self, delay):
        # add raise notimplemented
        pass
        if not delay:
            pass
        else:
            self.w3c_actions.key_action.pause(float(delay/1000))
            self.w3c_actions.pointer_action.pause(float(delay/1000))
        return self



    def send_keys(self, *keys):
        #overwrites ActionChain send_keys
        typing = keys_to_typing(keys)
        for key in typing:
            self.key_down(key)
            self.sleep_func(randint(0, 30)*10)
            self.key_up(key)
            self.sleep_func(randint(3, 10)*100)
        return self

    def human_move_to_element_from_pos(self, pos, end_elm):
        raise NotImplemented
        start_elm_x = pos[0]
        start_elm_y = pos[1]

        start_pad_x = 0
        start_pad_y = 0

        end_elm_x = end_elm.location['x']
        end_elm_y = end_elm.location['y']

        diff = end_elm_y - start_elm_y

        flip = -1
        if diff < 0: #checks if end position is lower than start
            flip = 1


        end_pad_x = end_elm.size['width']/2 + flip
        end_pad_y = end_elm.size['height']/2  + flip


        start_pos = [start_elm_x+start_pad_x, start_elm_y+start_pad_y]

        end_pos = [end_elm_x+end_pad_x, end_elm_y+end_pad_y]

        #self.move_to_element(start_elm)
        mid_pos = [start_pad_x*2, start_pad_y*2]


        self.human_move_to(start_pos, mid_pos, end_pos)
        return self

    def human_move_to_element_from(self, start_elm, end_elm):
        #start_elm_x = start_elm.location['x']
        #start_elm_y = start_elm.location['y']

        start_elm_x = start_elm.location['x']
        start_elm_y = start_elm.location['y']



        start_pad_x = start_elm.size['width']/2
        start_pad_y = start_elm.size['height']/2

        #start_pad_x = 0
        #start_pad_y = 0


        end_elm_x = end_elm.location['x']
        end_elm_y = end_elm.location['y']

        diff = end_elm_y - start_elm_y

        flip = -1
        if diff < 0: #checks if end position is lower than start
            flip = 1


        end_pad_x = end_elm.size['width']/2 + flip
        end_pad_y = end_elm.size['height']/2  + flip



        start_pos = [start_elm_x+start_pad_x, start_elm_y+start_pad_y]

        end_pos = [end_elm_x+end_pad_x, end_elm_y+end_pad_y]

        self.move_to_element(start_elm)
        mid_pos = [start_pad_x*2, start_pad_y*2]



        #start_pos = [x-canvas_x,y-canvas_y]


        self.human_move_to(start_pos, mid_pos, end_pos)
        return self


    def human_move_to(self, start_pos:list, mid_pos:list, end_pos:list):
        mid_x = start_pos[0]
        mid_y = start_pos[1]

        if mid_x < end_pos[0]:
            mid_x = end_pos[0]
        if mid_y < end_pos[1]:
            mid_y = end_pos[1]



        points = self.bezier_quad_curve(start_pos, mid_pos, end_pos)
        change = self.get_relative_change_of_points(points)
        #print("start_pos: {} mid_pos: {} end_pos: {}\n{}".format(start_pos, mid_pos, end_pos, change))

        is_below = 0<(end_pos[1] - start_pos[1])
        move_by = 1
        if not is_below:
            move_by = -1

        for x, y in change:
            #f = lambda: self._driver.execute_script("window.scrollBy(0, {})".format(move_by))
            #self._actions.append(f)
            self.move_by_offset(round(x), round(y))

        return self


    @staticmethod
    def bezier_quad_curve(pos1, pos2, pos3):
        #https://en.wikipedia.org/wiki/B%C3%A9zier_curve#Quadratic_B%C3%A9zier_curves
        p0, p1, p2 = np.array([pos1, pos2, pos3])
        func = lambda t: (1 - t)**2 * p0 + 2 * t * (1 - t) * p1 + t**2 * p2
        points = np.array([func(t) for t in np.linspace(0, 1, 50)])
        return points

    @staticmethod
    def get_relative_change_of_points(points, inverse=False):
        change = []
        x_abs = 0
        y_abs = 0
        for i in range(1, len(points)):
            x_change = points[i][0]-points[i-1][0]
            y_change = points[i][1]-points[i-1][1]
            if inverse:
                x_change*=-1
                y_change*=-1
            change.append([x_change, y_change])
            x_abs+=x_change
            y_abs+=y_change
        #print("x_abs {} y_abs {}".format(x_abs, y_abs))
        return change


class BrowserSession (object):
    user_agent = None
    has_proxy = False
    is_proxy_backconnect = False
    _proxy_ip = None
    _proxy_port = None
    _proxy_username = None
    _proxy_password = None


    def __init__(self, user_agent, window_size=[1920,1080], cookies=[], chrome_options=[], add_opts=False):
        self.user_agent = user_agent
        self.cookies = cookies

        self.chrome_options = chrome_options
        self.window_size = window_size

        if add_opts:
            self.chrome_options.append("user-agent={}".format(user_agent))
            self.chrome_options.append('--disable-extensions')
            #self.chrome_options.append("--incognito")
            self.chrome_options.append("--disable-plugins-discovery")
            self.chrome_options.append("--disable-blink-features=AutomationControlled")




    @staticmethod
    def random_chrome_options():
        options = []
        canvas_options = [
            "--disable-yuv-image-decoding", "--disable-yuv420-biplanar", "--disable-vulkan-surface",
            "--disable-accelerated-2d-canvas", "--disable-partial-raster","--disable-zero-copy",
            "--show-fps-counter", "--use-fake-mjpeg-decode-accelerator",  "--disable-gpu-rasterization" ,
            "--disable-low-res-tiling", "--disable-partial-raster", "--disable-software-rasterizer",
            "--enable-font-antialiasing", "--enable-webrtc-stun-origin", "--enable-win7-webrtc-hw-h264-decoding"
        ]
        gpu_options = [
            "--gpu-rasterization-msaa-sample-count=0", "--enable-native-gpu-memory-buffers=0"
        ]
        webgl_options = [
            "--webgl-antialiasing-mode=explicit"
        ]

        webrtc_options = [
            "--disable-webrtc-encryption", "--disable-webrtc-hw-decoding", "--disable-webrtc-hw-encoding",
            "--disable-rtc-smoothness-algorithm"
        ]

        screen_size_options = [
            "--window-size=1920,1080", "--window-size=1600,900"
        ]

        co_cnt = random.randint(0, len(canvas_options))-1
        while co_cnt > 0:
            co = canvas_options.pop(random.randint(0, len(canvas_options))-1)
            options.append(co)
            co_cnt-=1


        wo_cnt = random.randint(0, len(webrtc_options))-1
        while wo_cnt > 0:
            wo = webrtc_options.pop(random.randint(0, len(webrtc_options))-1)
            options.append(wo)
            wo_cnt-=1


        go_cnt = random.randint(0, len(gpu_options))-1
        while go_cnt > 0:
            go = gpu_options.pop(random.randint(0, len(gpu_options))-1)
            options.append(go)
            go_cnt-=1


        return options

    def set_proxy(self, ip, port, username=None, password=None, is_backconnect=False, overwrite=False):
        if self.has_proxy and not overwrite:
            raise ValueError("Proxy has already been set and overwrite is False")

        self._proxy_ip = ip
        self._proxy_port = port

        self._proxy_username = username
        self._proxy_password = password
        self.has_proxy = True

    def get_chrome_options(self, headless=False):
        options = Options()
        options.add_argument("--window-size={},{}".format(self.window_size[0], self.window_size[1]))

        if headless:
            options.add_argument("--headless");
            options.add_argument("--start-maximized")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-features=VizDisplayCompositor")
            options.add_argument("--use-fake-ui-for-media-stream=1")

        prefs = {
            "profile.default_content_setting_values.plugins": 1,
            "profile.content_settings.plugin_whitelist.adobe-flash-player": 1,
            "profile.content_settings.exceptions.plugins.*,*.per_resource.adobe-flash-player": 1,
            "PluginsAllowedForUrls": "*"
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)


        for opt in self.chrome_options:
            options.add_argument(opt)

        return options

    @staticmethod
    def load_json(data:str):
        data = json.loads(data)
        sesh = BrowserSession(data['user_agent'], cookies=data['cookies'], chrome_options=data['chrome_options'], add_opts=False)
        if data['proxy']:
            sesh.set_proxy(
                    data['proxy']['ip'],
                    data['proxy']['port'],
                    username=data['proxy']['username'],
                    password=data['proxy']['password'],
                    is_backconnect=data['proxy']['is_backconnect']
                )
        return sesh

    def dump(self):
        data = {
            'user_agent': self.user_agent,
            'chrome_options': self.chrome_options,
            'cookies': self.cookies,
            'proxy': None,
            'window_size': self.window_size
        }

        if self.has_proxy:
            data['proxy'] = {
                'ip':self._proxy_ip,
                'port': self._proxy_port,
                'username': self._proxy_username,
                'password': self._proxy_password,
                "is_backconnect": self._proxy_is_backconnect
            }

        return json.dumps(data)

    def update(self, browser):
        self.cookies = browser.get_cookies()
        #update cookie here etc
        pass

    @classmethod
    def generate(cls, browsers=['Chrome', 'Firefox', 'Edge'], os=['Mac', 'Windows'], file_path=None):
        coptions = cls.random_chrome_options()
        window_size = [1920, 1080]

        browser_types = []
        os_types = []
        popularity_types = [Popularity.POPULAR.value]
        for o in os:
            if o  == 'Windows':
                os_types.append(OperatingSystem.WINDOWS.value)
            elif o == 'Mac':
                os_types.append(OperatingSystem.MAC_OS_X.value)

        for b in browsers:
            if b == 'Chrome':
                browser_types.append(SoftwareName.CHROME.value)

            elif b == 'Edge':
                browser_types.append(SoftwareName.EDGE.value)

            elif b == 'Firefox':
                browser_types.append(SoftwareName.FIREFOX.value)

        ua_generator = RandomUserAgent(
            file_path=file_path,
            software_names=browser_types,
            operating_systems=os_types,
            popularity=popularity_types,
            limit=50
        )
        ua = ua_generator.get_random_user_agent()

        return BrowserSession(ua, chrome_options=coptions, window_size=window_size)



