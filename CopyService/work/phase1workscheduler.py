__author__ = 'alextc'
import logging
import datetime
from fs.fsutils import FsUtils
from isiapi.getsmblockscommand import GetSmbLocksCommand
from aop.logstartandexit import LogEntryAndExit
from model.phase1workitem import Phase1WorkItem
from sql.phase1db import Phase1Db


class Phase1WorkScheduler(object):

    _phase1_glob_query = "/ifs/zones/*/copy_svc/to/*/*/"
    _phase1_root = "/ifs/zones/"
    _mtime_stillness_threshold_in_sec = 3
    _smb_write_lock_stillness_threshold_in_sec = 3

    def __init__(self):
        pass

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def update_phase1_db(phase1_source_dirs, smb_write_locks):
        """
        :type phase1_source_dirs: list[str]
        :type smb_write_locks: list[str]
        :rtype: list[Phase1WorkItem]
        """

        for phase1_source_dir in phase1_source_dirs:
            # TODO: Optimize - get both mtime and ctime at the same time
            ctime = FsUtils.try_to_get_dir_created_time(phase1_source_dir)
            mtime = FsUtils.get_tree_mtime(phase1_source_dir)
            smb_write_lock_last_seen = \
                Phase1WorkScheduler.get_new_smb_write_lock_value(
                    phase1_source_dir,
                    ctime,
                    smb_write_locks)

            Phase1Db().add_or_update_work_item(
                Phase1WorkItem(
                    source_dir=phase1_source_dir,
                    tree_creation_time=ctime,
                    tree_last_modified=mtime,
                    smb_write_lock_last_seen=smb_write_lock_last_seen))

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _get_smb_write_locks(phase1_source_dirs):
        smb_write_locks = \
            GetSmbLocksCommand(trim_to_paths=phase1_source_dirs).execute()
        return smb_write_locks

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _get_phase1_source_dirs():
        phase1_source_dirs = \
            FsUtils.get_source_directories(Phase1WorkScheduler._phase1_glob_query)
        return phase1_source_dirs

    @staticmethod
    def get_new_smb_write_lock_value(phase1_source_dir, ctime, smb_write_locks):
        record_in_db = Phase1Db().get_work_item(phase1_source_dir, ctime)

        # This is the first time we see this dir so setting smb_write_lock to now
        if not record_in_db:
            smb_write_lock_last_seen = datetime.datetime.now()
        # dir has smb write lock so we need to reflect this in db
        elif phase1_source_dir in smb_write_locks:
            smb_write_lock_last_seen = datetime.datetime.now()
        else:
            # this is an existing record that is not locked, so will keep the prior value in db
            smb_write_lock_last_seen = record_in_db.last_smb_write_lock

        return smb_write_lock_last_seen