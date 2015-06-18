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
    def update_phase1_db():
        """
        :rtype: list[Phase1WorkItem]
        """

        phase1_db = Phase1Db()
        phase1_source_dirs = \
            FsUtils.get_source_directories(Phase1WorkScheduler._phase1_glob_query)
        smb_write_locks = \
            GetSmbLocksCommand(trim_to_paths=phase1_source_dirs).execute()

        for phase1_source_dir in phase1_source_dirs:
            ctime = FsUtils.try_to_get_dir_created_time(phase1_source_dir)
            mtime = FsUtils.get_tree_mtime(phase1_source_dir)

            record_in_db = phase1_db.get_work_item(phase1_source_dir, ctime)

            # This is the first time we see this dir so setting smb_write_lock to now
            if not record_in_db:
                smb_write_lock_last_seen = datetime.datetime.now()
            # dir has smb write lock so we need to reflect this in db
            elif phase1_source_dir in smb_write_locks:
                smb_write_lock_last_seen = datetime.datetime.now()
            else:
                # this is an existing record that is not locked, so will keep the prior value in db
                smb_write_lock_last_seen = record_in_db.last_smb_write_lock

            phase1_db.add_or_update_work_item(
                Phase1WorkItem(
                    source_dir=phase1_source_dir,
                    tree_creation_time=ctime,
                    tree_last_modified=mtime,
                    smb_write_lock_last_seen=smb_write_lock_last_seen))