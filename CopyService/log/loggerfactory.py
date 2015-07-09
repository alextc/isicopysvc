__author__ = 'alextc'
import logging
import logging.handlers as handlers
import os
import socket


class LoggerFactory(object):

    _log_base_path = "/ifs/copy_svc/"

    @staticmethod
    def create(log_name, logging_level = logging.DEBUG):
        logger = logging.getLogger(log_name)
        logger.setLevel(logging_level)
        logger.addHandler(LoggerFactory._create_file_handler(log_name))
        logger.addHandler(LoggerFactory._create_sys_log_handler('192.168.11.50', 514))
        return logger

    @staticmethod
    def _create_file_handler(log_name):
        file_handler = logging.FileHandler(LoggerFactory._create_log_file_path(log_name))
        # file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter("[%(asctime)s %(process)s] %(message)s")
        file_handler.setFormatter(file_formatter)
        return file_handler

    @staticmethod
    def _create_sys_log_handler(hostname, port):
        syslog_handler = handlers.SysLogHandler(address=(hostname, port))
        syslog_formatter = logging.Formatter('%(name)s[%(process)d]: %(levelname)-8s %(message)s')
        syslog_handler.setFormatter(syslog_formatter)
        return syslog_handler

    @staticmethod
    def _create_log_file_path(log_name):
        log_file_name = "{0}_{1}.{2}".format(socket.gethostname(), log_name, "log")
        log_path = os.path.join(LoggerFactory._log_base_path, log_file_name)

        # Clearing the log - comment out in production
        if os.path.exists(log_path):
            os.remove(log_path)

        return log_path
