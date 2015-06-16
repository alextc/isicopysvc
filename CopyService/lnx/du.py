__author__ = 'alextc'
from subprocess import Popen, PIPE
import logging


class DiskUsage(object):
    def __init__(self):
        pass

    @staticmethod
    def get_disk_usage(path):
        process = Popen(['du', '-s', path], stdout=PIPE, stderr=PIPE)
        # BUG: this below cuts out 1st character, ex. instead of 83 returns 3
        stdout, notused = process.communicate()
        result = stdout.split()[0].decode('utf-8')
        return int(result)
