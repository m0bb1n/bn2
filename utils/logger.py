import logging
import logging.handlers
from termcolor import colored
from sys import _getframe

class ColorFormatter (logging.Formatter):
    def __init__(self, fmt, time_fmt=None):
        fmt = fmt.replace('%(levelname)s', '__LEVEL__')
        if time_fmt:
            logging.Formatter.__init__(self, fmt, time_fmt)
        else:
            logging.Formatter.__init__(self, fmt)




    def format(self, record):
        format_orig = self._fmt
        color = None

        if record.levelno == logging.DEBUG:
            color = colored('DEBUG', 'grey', 'on_grey', attrs=['bold'])

        elif record.levelno == logging.INFO:
            color = colored('INFO', 'blue', 'on_grey', attrs=['bold'])

        elif record.levelno == logging.WARNING:
            color = colored('WARNING', 'yellow', 'on_grey', attrs=['bold'])

        elif record.levelno == logging.ERROR:
            color = colored('ERROR', 'red', 'on_grey', attrs=['bold'])

        elif record.levelno == logging.CRITICAL:
            color = colored('CRITICAL', 'red', 'on_yellow', attrs=['bold'])


        result = logging.Formatter.format(self, record)
        return result.replace("__LEVEL__", color)

        self._fmt = format_orig


class Logger (object):
    check=True

    def __init__(self, path, color=False, to_file=None):
        self.__logger = logging.getLogger('bot'+__name__)
        self.__logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)

        if color:
            obj = ColorFormatter
        else:
            obj = logging.Formatter
        formatter = obj('[%(asctime)s]>[%(levelname)s]>[%(path)s]: %(message)s', "%m-%d %H:%M:%S")
        if to_file:
            out = '"t":"{}","l":"{}","p":"{}","m":"{}"'.format(
                "%(asctime)s",
                "%(levelname)s",
                "%(path)s",
                "%(message)s")

            file_formatter = logging.Formatter('{'+out+'},', "%m-%d %H:%M:%S") #completes json
            file_handler = logging.handlers.WatchedFileHandler(to_file)
            file_handler.setFormatter(file_formatter)
            self.__logger.addHandler(file_handler)

        stream_handler.setFormatter(formatter)

        self.__logger.addHandler(stream_handler)

        self.logger_data = {"path": path}
        self.color = color


    def set_path(self, path, check=True):
        self.logger_data['path'] = path
        self.check = check

    def debug(self, msg, path=None):
        data = self.logger_data
        if not path and self.check:
            route_meta = None
            try:
                route_meta = _getframe(1).f_locals['route_meta']
            except:
                pass
            else:
                path = route_meta['route']

        if path:
            data = {'path': path}

        self.__logger.debug(msg, extra=data)


    def info(self, msg, path=None):
        data = self.logger_data

        if not path and self.check:
            route_meta = None
            try:
                route_meta = _getframe(1).f_locals['route_meta']
            except:
                pass
            else:
                path = route_meta['route']

        if path:
            data = {'path': path}

        self.__logger.info(msg, extra=data)


    def warning(self, msg, path=None):
        data = self.logger_data
        if not path and self.check:
            route_meta = None
            try:
                route_meta = _getframe(1).f_locals['route_meta']
            except:
                pass
            else:
                path = route_meta['route']

        if path:
            data = {'path': path}

        self.__logger.warning(msg, extra=data)

    def error(self, msg, path=None):
        data = self.logger_data
        if not path and self.check:
            route_meta = None
            try:
                route_meta = _getframe(1).f_locals['route_meta']
            except:
                pass
            else:
                path = route_meta['route']

        if path:
            data = {'path': path}

        self.__logger.error(msg, extra=data)

    def critical(self, msg, path=None):
        data = self.logger_data
        if not path and self.check:
            route_meta = None
            try:
                route_meta = _getframe(1).f_locals['route_meta']
            except:
                pass
            else:
                path = route_meta['route']

        if path:
            data = {'path': path}

        self.__logger.critical(msg, extra=data)



