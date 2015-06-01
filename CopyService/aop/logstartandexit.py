__author__ = 'alextc'
#https://wiki.python.org/moin/PythonDecoratorLibrary#Logging_decorator_with_specified_logger_.28or_default.29

import functools
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class LogEntryAndExit(object):
    '''Logging decorator that allows you to log with a specific logger. '''
    # Customize these messages
    ENTRY_MESSAGE = 'ENTERING {}'
    EXIT_MESSAGE = 'EXITING {}'

    def __init__(self, logger=None):
        self.logger = logger

    def __call__(self, func):
        '''Returns a wrapper that wraps func. The wrapper will log the entry and exit points of the function
        with logging.INFO level. '''
        # set logger if it was not set earlier
        if not self.logger:
            logging.basicConfig()
            self.logger = logging.getLogger(func.__module__)

        @functools.wraps(func)
        def wrapper(*args, **kwds):
            self.logger.debug(self.ENTRY_MESSAGE.format(func.__name__))
            f_result = func(*args, **kwds)
            self.logger.debug(self.EXIT_MESSAGE.format(func.__name__))
            return f_result
        return wrapper