__author__ = 'alextc'
from subprocess import Popen, PIPE
import logging


class ProcessList(object):
    def __init__(self):
        self._main_file_name = "phase2.py"

    def get_number_of_running_instances(self):
        number_of_instances = 0
        process = Popen(['ps', '-f'], stdout=PIPE, stderr=PIPE)
        stdout, notused = process.communicate()
        for line in stdout.splitlines():
            pid, cmdline = line.split(' ', 1)
            # logging.debug("{0}".format(cmdline))
            if "python " + self._main_file_name in cmdline:
                number_of_instances += 1

        logging.debug("Returning {0}".format(number_of_instances))
        return number_of_instances
