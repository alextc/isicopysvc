__author__ = 'alextc'
import logging
import shutil
from aop.logstartandexit import LogEntryAndExit
from sql.phase1db import Phase1Db
from model.phase1workitem import Phase1WorkItem


class Phase1Worker(object):

    _smb_write_lock_stillness_threshold_in_sec = 3

    def __init__(self):
        pass

    @staticmethod
    def run():
        still_work_items = Phase1Worker._get_still_work_times_from_db()
        Phase1Worker._move_still_items_to_staging(still_work_items)
        Phase1Worker._remove_processed_items_from_db(still_work_items)

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _get_still_work_times_from_db():
        """
        :rtype: list[Phase1WorkItem]
        """
        still_dirs = \
            Phase1Db().get_still_work_items(Phase1Worker._smb_write_lock_stillness_threshold_in_sec)
        return still_dirs

    @staticmethod
    def _move_still_items_to_staging(still_items):
        """
        :type still_items: list[Phase1WorkItem]
        :return:
        """
        for still_item in still_items:
            shutil.move(still_item.phase1_source_dir, still_item.phase2_staging_dir)

    @staticmethod
    def _remove_processed_items_from_db(still_work_items):
        """
        :type still_work_items: list[Phase1WorkItem]
        :return:
        """
        for still_work_item in still_work_items:
            Phase1Db().remove_work_item(still_work_item)