__author__ = 'alextc'
import logging
import datetime
from fs.fsutils import FsUtils
# from isiapi.getsmblockscommand import GetSmbLocksCommand
from common.datetimeutils import DateTimeUtils
from aop.logstartandexit import LogEntryAndExit


class Phase1WorkScheduler(object):

    _phase1_glob_query = "/ifs/zones/*/to/*/*/"
    _mtime_stillness_threshold_in_sec = 3

    def __init__(self):
        pass

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _is_mtime_stillness_threshold_reached(path):
        """
        :param path:
        :rtype: bool
        """
        logging.debug("About to determine stillness for {0}".format(path))
        mtime = FsUtils.get_tree_mtime(path)
        logging.debug("mtime on {0} is {1}".format(path, mtime))
        # TODO: Confirm utcnow is correct
        result = \
            DateTimeUtils.get_total_seconds_for_timedelta(datetime.datetime.now() - mtime) > \
            Phase1WorkScheduler._mtime_stillness_threshold_in_sec
        return result

    @staticmethod
    def _is_smb_write_lock_applied(path):
        return False

    def get_still_dirs(self):
        potential_phase1_work_inputs = FsUtils.get_source_directories(Phase1WorkScheduler._phase1_glob_query)
        still_dirs_based_on_mtime = \
            filter(lambda d: Phase1WorkScheduler._is_mtime_stillness_threshold_reached(d),
                   potential_phase1_work_inputs)
        still_dirs_based_on_mtime_and_smb_lock = \
            filter(lambda d: not self._is_smb_write_lock_applied(d), still_dirs_based_on_mtime)

        return still_dirs_based_on_mtime_and_smb_lock






