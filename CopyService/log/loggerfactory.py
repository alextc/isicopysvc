__author__ = 'alextc'
import logging
import logging.handlers as handlers
import os
import socket
from bases.configurableobject import ConfigurableObject


class LoggerFactory(ConfigurableObject):

    _log_base_path = "/ifs/copy_svc/"

    def create(self, log_name):
        logger = logging.getLogger(log_name)
        logger.setLevel(self._get_logging_level())
        logger.addHandler(LoggerFactory._create_file_handler(log_name))
        logger.addHandler(
            LoggerFactory._create_sys_log_handler(
                self._get_syslog_server(),
                self._get_syslog_server_port()))
        return logger

    def _get_logging_level(self):
        log_level = self.__class__._config.get('Logging', 'Level')
        assert log_level, "Failed to read log_level from config"

        if log_level == 'CRITICAL':
            return logging.CRITICAL
        elif log_level == 'ERROR':
            return logging.ERROR
        elif log_level == 'WARNING':
            return logging.WARNING
        elif log_level == 'INFO':
            return logging.INFO
        elif log_level == 'DEBUG':
            return logging.DEBUG
        else:
            raise ValueError("Unexpected Log Level: {0}".format(log_level))

    def _get_syslog_server(self):
        syslog_server = self.__class__._config.get('Logging', 'SyslogServer')
        # TODO: do regex to validate that this value is a hostname or IP
        return syslog_server

    def _get_syslog_server_port(self):
        syslog_server_port = self.__class__._config.getint('Logging', 'SyslogPort')
        return syslog_server_port

    @staticmethod
    def _create_file_handler(log_name):
        file_handler = logging.FileHandler(LoggerFactory._create_log_file_path(log_name))
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
