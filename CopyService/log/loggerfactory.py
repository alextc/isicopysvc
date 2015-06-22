__author__ = 'alextc'
import logging
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
        formatter = logging.Formatter("[%(asctime)s %(process)s] %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger