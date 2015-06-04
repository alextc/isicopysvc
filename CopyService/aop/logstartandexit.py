__author__ = 'alextc'
# https://wiki.python.org/moin/PythonDecoratorLibrary#Logging_decorator_with_specified_logger_.28or_default.29
import functools

class LogEntryAndExit(object):
    ENTRY_MESSAGE = 'ENTERING {0}'
    EXIT_MESSAGE = 'EXITING {0}'

    def __init__(self, logger):
        self.logger = logger
        print logger

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwds):
            self.logger.debug(self.ENTRY_MESSAGE.format(func.__name__))
            f_result = func(*args, **kwds)
            self.logger.debug(self.EXIT_MESSAGE.format(func.__name__))
            return f_result
        return wrapper