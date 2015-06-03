__author__ = 'alextc'
import logging
import os
import time
import fcntl
import sys
import socket
from lnx.processlist import ProcessList

class ProcessPool(object):
    def __init__(self):
        self._process_pool_entrance_lock = "/ifs/copy_svc/" + socket.gethostname() + "_" + "pool.lock"
        self._max_retry_count = 10
        self._max_concurrent = 5

    def is_max_process_count_reached(self):
        logging.debug("Entered is_max_process_count_reached")
        assert os.path.exists(os.path.dirname(self._process_pool_entrance_lock)), "Process file directory not found"

        ps = ProcessList()

        for i in range(self._max_retry_count):
            try:
                with open(self._process_pool_entrance_lock, 'w+') as process_pool_entrance_lock_handle:
                    fcntl.flock(process_pool_entrance_lock_handle.fileno(), fcntl.LOCK_EX)

                    num_of_running_instances = ps.get_number_of_running_instances()
                    logging.debug("ps returned {0}".format(num_of_running_instances))
                    is_max_reached = num_of_running_instances >= self._max_concurrent
                    logging.debug("is_max_process_count_reached returning {0}".format(is_max_reached))
                    return is_max_reached

            except IOError:
                logging.debug(sys.exc_info()[0])
                time.sleep(1)

        # TODO: check if the value of i is retained here, should the function ever get here
        assert i < self._max_retry_count, "Unable to open process file in max_process_running"
