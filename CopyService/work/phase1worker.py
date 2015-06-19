__author__ = 'alextc'
import logging
from aop.logstartandexit import LogEntryAndExit
from model.phase1workitem import Phase1WorkItem
from sql.phase1db import Phase1Db


class Phase1Worker(object):

    _smb_write_lock_stillness_threshold_in_sec = 3

    def __init__(self):
        pass

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _get_still_dirs():
        """
        :rtype: list[Phase1WorkItem]
        """
        still_dirs = \
            Phase1Db().get_still_work_items(Phase1Worker._smb_write_lock_stillness_threshold_in_sec)
        return still_dirs