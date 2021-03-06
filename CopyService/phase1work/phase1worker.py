__author__ = 'alextc'
import shutil
from sql.phase1db import Phase1Db
from model.phase1workitem import Phase1WorkItem
from log.loggerfactory import LoggerFactory


class Phase1Worker(object):
    _smb_write_lock_stillness_threshold_in_sec = 3
    _logger = LoggerFactory().create("Phase1Worker")

    def __init__(self):
        pass

    def run(self):
        still_work_items = self._get_still_work_items_from_db()
        if not still_work_items:
            Phase1Worker._logger.debug("No still items were detected exiting run")
            return
        else:
            self._logger.debug("Detected still items:")
            for still_work_item in still_work_items:
                self._logger.debug("\n{0}".format(still_work_item))

        self._move_still_items_to_staging(still_work_items)
        self._remove_processed_items_from_db(still_work_items)

    @staticmethod
    def _get_still_work_items_from_db():
        """
        :rtype: list[Phase1WorkItem]
        """
        still_dirs = \
            Phase1Db().get_still_work_items(Phase1Worker._smb_write_lock_stillness_threshold_in_sec)

        if not still_dirs:
            Phase1Worker._logger.debug("No still items were detected in _get_still_work_times_from_db")

        return still_dirs

    @staticmethod
    def _move_still_items_to_staging(still_items):
        """
        :type still_items: list[Phase1WorkItem]
        :return:
        """
        for still_item in still_items:
            shutil.move(still_item.phase1_source_dir, still_item.phase2_staging_dir)
            Phase1Worker._logger.debug("Moved {0} to {1}".format(
                still_item.phase1_source_dir,
                still_item.phase2_staging_dir))

    @staticmethod
    def _remove_processed_items_from_db(still_work_items):
        """
        :type still_work_items: list[Phase1WorkItem]
        :return:
        """
        for still_work_item in still_work_items:
            Phase1Db().remove_work_item(still_work_item)
            Phase1Worker._logger.debug(
                "Removed processed item from Db {0}".format(still_work_item.phase1_source_dir))
