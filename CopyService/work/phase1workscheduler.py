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
    def update_phase1_db(phase1_source_dirs=None, smb_write_locks=None):
        """
        :type phase1_source_dirs: list[str]
        :type smb_write_locks: list[str]
        :rtype: list[Phase1WorkItem]
        """

        if not phase1_source_dirs:
            phase1_source_dirs = Phase1WorkScheduler._get_phase1_source_dirs()

        if not smb_write_locks:
            smb_write_locks = Phase1WorkScheduler._get_smb_write_locks(phase1_source_dirs)

        for phase1_source_dir in phase1_source_dirs:
            # TODO: Optimize - get both mtime and ctime at the same time
            ctime = FsUtils.try_to_get_dir_created_time(phase1_source_dir)
            mtime = FsUtils.get_tree_mtime(phase1_source_dir)
            existing_record = Phase1WorkScheduler._get_existing_record(phase1_source_dir, ctime)
            smb_write_lock_last_seen = \
                Phase1WorkScheduler._get_new_smb_write_lock_value(
                    phase1_source_dir,
                    mtime,
                    smb_write_locks,
                    existing_record)

            phase1_work_item = Phase1WorkItem(
                        source_dir=phase1_source_dir,
                        tree_creation_time=ctime,
                        tree_last_modified=mtime,
                        smb_write_lock_last_seen=smb_write_lock_last_seen)

            if existing_record:
                Phase1Db().update_work_item(phase1_work_item)
            else:
                Phase1Db().add_work_item(phase1_work_item)

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
            FsUtils.glob(Phase1WorkScheduler._phase1_glob_query)
        return phase1_source_dirs

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _get_existing_record(phase1_source_dir, ctime):
        logging.debug("Attempting to retrieve record for {0} from db".format(phase1_source_dir))
        record_in_db = Phase1Db().get_work_item(phase1_source_dir, ctime)
        if record_in_db:
            logging.debug("Successfully retrieved db record")
            logging.debug("{0}".format(record_in_db))
            return record_in_db
        else:
            logging.debug("No db record found")
            return None

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def _get_new_smb_write_lock_value(phase1_source_dir, mtime, smb_write_locks, existing_record):

        # This is the first time we see this dir so setting smb_write_lock to now
        if not existing_record:
            smb_write_lock_last_seen = mtime
        # dir has smb write lock so we need to reflect this in db
        elif phase1_source_dir in smb_write_locks:
            smb_write_lock_last_seen = datetime.datetime.now()
        else:
            # this is an existing record that is not locked, so will keep the prior value in db
            smb_write_lock_last_seen = existing_record.last_smb_write_lock

        return smb_write_lock_last_seen