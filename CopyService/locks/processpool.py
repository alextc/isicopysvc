__author__ = 'alextc'
import logging
import os
import time
import fcntl
import sys
import socket

class ProcessPool(object):
    def __init__(self):
        self._process_records_file = "/ifs/copy_svc/" + socket.gethostname() + "_" + "proc.db"
        self._max_retry_count = 10
        self._max_concurrent = 5

    def is_max_process_count_reached(self):
        logging.debug("Entered is_max_process_count_reached")
        assert os.path.exists(os.path.dirname(self._process_records_file)), "Process file directory not found"

        # First call scenario - no file yet
        if not os.path.isfile(self._process_records_file):
            logging.debug("process_file does not exist; Assuming this is first run, returning False")
            return False

        for i in range(self._max_retry_count):
            try:
                with open(self._process_records_file) as process_info:
                    # fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(process_info.fileno(), fcntl.LOCK_EX)
                    processes = process_info.readlines()
                    assert len(processes) < self._max_concurrent + 1, "Exceeded max_concurrent processes value"
                    is_max_reached = len(processes) >= self._max_concurrent
                    logging.debug("is_max_process_count_reached returning {0}".format(is_max_reached))
                    return is_max_reached
            except IOError:
                logging.debug(sys.exc_info()[0])
                time.sleep(1)

        # TODO: check if the value of i is retained here, should the function ever get here
        assert i < self._max_retry_count, "Unable to open process file in max_process_running"

    def increment_process_running_count(self):
        logging.debug("Entered increment_process_running_count")
        assert os.path.exists(os.path.dirname(self._process_records_file)), "Process file directory not found"

        for i in range(self._max_retry_count):
            try:
                logging.debug("About to request lock for {0}".format(self._process_records_file))
                with open(self._process_records_file, 'a+') as process_info:
                    fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    current_process_info = str(os.getpid())
                    process_info.writelines("{0}\n".format(current_process_info))
                    logging.debug("About to update process_file with {0}".format(current_process_info))
                logging.debug("Incremented process count")
                return
            except IOError:
                logging.debug(sys.exc_info()[0])
                time.sleep(1)

        assert i < self.max_retry_count, "Unable to update process count in add_process_running"

    def get_current_process_count(self):
        logging.debug("Entered get_current_process_count")
        assert os.path.exists(os.path.dirname(self._process_records_file)), "Process file directory not found"

        # First call scenario - no file yet
        if not os.path.isfile(self._process_records_file):
            logging.debug("Returning 0 since process file does not exist")
            return 0

        with open(self._process_records_file) as process_info:
            processes = process_info.readlines()
            if not processes:
                return 0
            else:
                return len(processes)

    def decrement_process_count(self):
        assert os.path.exists(self._process_records_file), \
            "Attempt to open a non-existing process file in decrement_process_count"
        for i in range(self._max_retry_count):
            try:
                with open(self._process_records_file, 'r+') as process_info:
                    fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lines = process_info.readlines()
                    assert len(lines) > 0, "Was asked to remove a process from an empty file"
                    lines.pop()
                    process_info.writelines(lines)
                    logging.debug("Decremented process count, new value is {}".format(len(lines)))
            except:
                logging.debug(sys.exc_info()[0])
                time.sleep(1)

        assert i < self._max_retry_count, "Unable to decrement process count in decrement_process_count"
