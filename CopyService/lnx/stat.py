__author__ = 'alextc'

from subprocess import Popen, PIPE
import logging


class Stat(object):
    def __init__(self):
        pass

    @staticmethod
    def get_birth_time(path):
        # TODO: How to pass multiple parameters
        process = Popen(['stat', "-f '%SB' ", path], stdout=PIPE, stderr=PIPE)

        stdout, notused = process.communicate()
        result = stdout.split()[0].decode('utf-8')

        # TODO: Convert to datetime