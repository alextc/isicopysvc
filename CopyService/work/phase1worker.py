__author__ = 'alextc'
import logging
import datetime
from fs.fsutils import FsUtils
from isiapi.getsmblockscommand import GetSmbLocksCommand
from common.datetimeutils import DateTimeUtils
from aop.logstartandexit import LogEntryAndExit
from model.phase1workitem import Phase1WorkItem


class Phase1Worker(object):

    _phase1_glob_query = "/ifs/zones/*/to/*/*/"
    _phase1_root = "/ifs/zones/"
    _mtime_stillness_threshold_in_sec = 3
    _smb_write_lock_stillness_threshold_in_sec = 3

    def __init__(self):
        pass





