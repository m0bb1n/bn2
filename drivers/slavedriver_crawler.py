from bn2.drivers.slavedriver import SlaveDriver
from bn2.utils.crawler_tools import HumanActions, BrowserSession
from bn2.utils.msgqueue \
    import  create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG

import psutil

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os.path

from functools import reduce
import operator

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from random import randint

AnticaptchaClient = None
NoCaptchaTaskProxylessTask = None
Display = None


#from pyvirtualdisplay import Display
from python3_anticaptcha.NoCaptchaTaskProxyless import NoCaptchaTaskProxyless

class CrawlerDriver (SlaveDriver):
    WEBDRIVERS_ARE_HEADLESS = False
    def __init__(self, config):
        super(CrawlerDriver, self).__init__(config)

        self._init_crawler_configs()
        self.ac_api_key = self.get_bot_config('anticaptcha_api_key')
        if self.ac_api_key:
            self.ac_recaptchav2 = NoCaptchaTaskProxyless(anticaptcha_key=self.ac_api_key)
            self.log.debug("Anti-Captcha Client key api key received", path='bd.@sd.init')


    def _init_crawler_configs(self) -> None:

        self.add_bot_config(
            'anticaptcha_api_key',
            'Anti-Captcha Api Key',
            'str',
            desc='Find api key in Anti-Captcha dashboard'
        )

        self.add_bot_config(
            'screenshot_dir',
            'Directory to store screenshots',
            'path'
        )

        self.add_bot_config(
            'is_headless',
            'Crawler is headless',
            'bool',
            default=True
        )

        self.add_bot_config(
            'chromedriver_exe_path',
            'Chromedriver executable path',
            'path',
            default='%%/resources/chromedriver',
            desc='Path of chromedriver executable'
        )

        self.add_bot_config(
            'user_agents_path',
            'User Agents zip path',
            'path',
            default='%%/resources/user_agents.zip'
        )

        self.load_bot_config_from_file()


    def get_recaptcha_site_key(self, browser):
        return browser.find_element_by_class_name("g-recaptcha").get_attribute(
        "data-sitekey"
        )

    def get_recaptcha_solution(self, site_key, url, *, max_delay=60, invisible=False, throw=True):
        result = self.ac_recaptchav2.captcha_handler(websiteURL=url, websiteKey=site_key)
        if result["errorId"]:
            self.log.error(result)
            if throw:
                raise Exception(result)
            return None
        return result['solution']['gRecaptchaResponse']

    def save_browser_screenshot(self, browser:webdriver, fn:str):
        path = fn
        dir_ = self.get_bot_config('screenshot_dir')
        if dir_:
            path = os.path.join(dir_, fn)
        browser.save_screenshot(path)

    def quit_browser(self, browser:webdriver) -> None:
        browser.quit()


        #self.inbox.put(msg, INBOX_SYS_MSG)


    def create_browser_from_session(self, sesh:BrowserSession) -> webdriver:
        headless = self.get_bot_config('is_headless')

        chrome_options = sesh.get_chrome_options(headless=headless)
        browser = self.create_browser(chrome_options, headless=headless)
        return browser

    def create_browser(self, chrome_options, headless=False):
        browser = webdriver.Chrome(self.get_bot_config('chromedriver_exe_path'), chrome_options=chrome_options)
        if self.RUNNING_GLOBAL_TASK and self.RUNNING_GLOBAL_TASK_PID==os.getpid():
            pid = browser.service.process.pid
            pids = [p.pid for p in psutil.Process(pid).children(recursive=True)]
            pids.append(pid)
            msg = self.create_local_task_message(
                'bd.@sd.task.global.pids',
                {"pids":pids}
            )
            self.inbox.put(msg, INBOX_SYS_MSG)

        return browser

    def create_human_actions(self, browser:webdriver) -> HumanActions:
        actions = HumanActions(browser)
        actions.set_sleep_func(self.sleep)
        return actions

    def generate_human_movement_slope(self):
        pass


    def human_send_keys_to_element(self, element, text:str, click:bool=True, clear:bool=False) -> None:
        #use actions
        self.random_sleep(_max=5000)
        if click:
            element.click()
            self.sleep(500)
        if clear:
            element.clear()

        if type(text) == str:
            for char in text:
                element.send_keys(char)
                self.random_sleep(_max=1000)
        else:
            element.send_keys(text)

    def random_sleep(self, _min:int=0, _max:int=1500, randomize_max:bool=False) -> None:
        if randomize_max:
            _max = randint(_min ,_max)
        rand = randint(_min ,_max)-1
        self.sleep(rand)
