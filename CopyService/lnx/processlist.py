__author__ = 'alextc'
from subprocess import Popen, PIPE
import logging
import os

class ProcessList(object):
    def __init__(self):
        pass

    def get_number_of_running_instances(self):
        number_of_instances = 0
        process = Popen(['ps', '-f'], stdout=PIPE, stderr=PIPE)
        stdout, notused = process.communicate()
        for line in stdout.splitlines():
            pid, cmdline = line.split(' ', 1)
            if "python " + os.path.basename(__file__) in cmdline:
                number_of_instances += 1
        logging.debug("Returning {0}".format(number_of_instances))
        return number_of_instances