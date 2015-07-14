__author__ = 'alextc'
import datetime
from subprocess import Popen, PIPE
from common.datetimeutils import DateTimeUtils


class Stat(object):
    def __init__(self):
        pass

    @staticmethod
    def get_birth_time(path):
        process = Popen(['stat', "-f '%SB' ", path], stdout=PIPE, stderr=PIPE)
        stdout, notused = process.communicate()
        result = stdout.decode('utf-8').strip().replace("'", "")
        # Jul 14 11:07:58 2015
        dir_birth_date = datetime.datetime.strptime(result, "%b %d %X %Y")
        return dir_birth_date