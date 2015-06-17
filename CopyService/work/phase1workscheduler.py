__author__ = 'alextc'
import logging
import datetime
from fs.fsutils import FsUtils
from isiapi.getsmblockscommand import GetSmbLocksCommand
from common.datetimeutils import DateTimeUtils
from aop.logstartandexit import LogEntryAndExit
from model.phase1workitem import Phase1WorkItem


class Phase1WorkScheduler(object):

    _phase1_glob_query = "/ifs/zones/*/to/*/*/"
    _phase1_root = "/ifs/zones/"
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
        result = \
            DateTimeUtils.get_total_seconds_for_timedelta(datetime.datetime.now() - mtime) > \
            Phase1WorkScheduler._mtime_stillness_threshold_in_sec
        logging.debug("Returning {0}".format(result))
        return result

    @staticmethod
    def get_potential_phase1_work_items():
        """
        :rtype: list[Phase1WorkItem]
        """
        phase1_source_dirs = \
            FsUtils.get_source_directories(Phase1WorkScheduler._phase1_glob_query)

        result = []
        for phase1_source_dir in phase1_source_dirs:
            mtime = FsUtils.get_source_directories(phase1_source_dir)
            result.append(Phase1WorkItem(
                source_dir=phase1_source_dir,
                tree_creation_time=mtime,
                tree_last_modified=mtime))

        # TODO: Enrich phase1_source_dirs with smb_write_lock_time
        # Logic
        # Check if dealing with a new directory based on name and time created
        # If new -> insert new record set smb_write_lock_time to ctime
        # If existing directory -> Get smb_write_lock info
            # if got smb_lock -> update record with new mtime and smb_lock_time
            # if no smb_lock -> update record with current mtime

        """
        still_dirs_based_on_mtime = \
            filter(lambda d: Phase1WorkScheduler._is_mtime_stillness_threshold_reached(d),
                   phase1_source_dirs)
        smb_locked_dirs = GetSmbLocksCommand(still_dirs_based_on_mtime).execute()
        return list(set(still_dirs_based_on_mtime).difference(set(smb_locked_dirs)))
        """

        return result

