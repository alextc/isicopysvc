__author__ = 'alextc'
import logging
import logging.handlers as handlers
import os


class LoggerFactory(object):

    _log_base_path = "/ifs/copy_svc/"

    @staticmethod
    def create(log_name):
        log_name += '.log'
        log_path = os.path.join(LoggerFactory._log_base_path, log_name)
        # Clearing the log - comment out in production
        if os.path.exists(log_path):
            os.remove(log_path)

        logger = logging.getLogger(log_name)
        # Do I need this - DEBUG is being set on file handler below
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)

        file_formatter = logging.Formatter("[%(asctime)s %(process)s] %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        syslog_handler = handlers.SysLogHandler(address=('172.16.0.2', 514))
        syslog_formatter = logging.Formatter('%(name)s[%(process)d]: %(levelname)-8s %(message)s')
        syslog_handler.setFormatter(syslog_formatter)
        logger.addHandler(syslog_handler)

        return logger
