__author__ = 'alextc'
import logging
import logging.handlers as handlers
import os
import socket


class LoggerFactory(object):

    _log_base_path = "/ifs/copy_svc/"

    @staticmethod
    def create(log_name):
        logger = logging.getLogger(log_name)
        # Do I need this - DEBUG is being set on file handler below
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(LoggerFactory._generate_log_file_path(log_name))
        file_handler.setLevel(logging.DEBUG)

        file_formatter = logging.Formatter("[%(asctime)s %(process)s] %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        syslog_handler = handlers.SysLogHandler(address=('192.168.11.50', 514))
        syslog_formatter = logging.Formatter('%(name)s[%(process)d]: %(levelname)-8s %(message)s')
        syslog_handler.setFormatter(syslog_formatter)
        logger.addHandler(syslog_handler)

        return logger

    @staticmethod
    def _generate_log_file_path(log_name):
        log_file_name = "{0}_{1}.{2}".format(socket.gethostname(), log_name, "log")
        log_path = os.path.join(LoggerFactory._log_base_path, log_file_name)

        # Clearing the log - comment out in production
        if os.path.exists(log_path):
            os.remove(log_path)

        return log_path
