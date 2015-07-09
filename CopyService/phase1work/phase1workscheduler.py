__author__ = 'alextc'
import datetime
from fs.fsutils import FsUtils
from isiapi.getsmblockscommand import GetSmbLocksCommand
from model.phase1workitem import Phase1WorkItem
from sql.phase1db import Phase1Db
from log.loggerfactory import LoggerFactory


class Phase1WorkScheduler(object):

    _phase1_glob_query = "/ifs/zones/*/copy_svc/to/*/*/"
    _phase1_root = "/ifs/zones/"
    _mtime_stillness_threshold_in_sec = 3
    _smb_write_lock_stillness_threshold_in_sec = 3
    _logger = LoggerFactory().create('Phase1WorkScheduler')

    def __init__(self):
        pass

    def run(self):
        phase1_source_dirs = self._get_phase1_source_dirs()
        smb_write_locks = self._get_smb_write_locks(phase1_source_dirs)
        self._update_phase1_db(phase1_source_dirs, smb_write_locks)

    def _update_phase1_db(self, phase1_source_dirs, smb_write_locks):
        """
        :type phase1_source_dirs: list[str]
        :type smb_write_locks: list[str]
        :rtype: list[Phase1WorkItem]
        """

        for phase1_source_dir in phase1_source_dirs:
            # TODO: Optimize - get both mtime and ctime at the same time
            mtime = FsUtils().get_tree_mtime(phase1_source_dir)
            existing_record = self._get_existing_record(phase1_source_dir)
            smb_write_lock_last_seen = \
                self._get_new_smb_write_lock_value(
                    phase1_source_dir,
                    mtime,
                    smb_write_locks,
                    existing_record)

            phase1_work_item = Phase1WorkItem(
                        source_dir=phase1_source_dir,
                        tree_last_modified=mtime,
                        smb_write_lock_last_seen=smb_write_lock_last_seen)

            if existing_record:
                Phase1Db().update_work_item(phase1_work_item)
            else:
                Phase1WorkScheduler._logger.debug(
                    "About to insert into Phase1Db new phase1_work_item\n{0}".format(phase1_work_item))
                Phase1Db().add_work_item(phase1_work_item)

    @staticmethod
    def _get_smb_write_locks(phase1_source_dirs):
        smb_write_locks = \
            GetSmbLocksCommand(trim_to_paths=phase1_source_dirs).execute()
        if smb_write_locks:
            Phase1WorkScheduler._logger.debug("Located SMB Write Locks on\n{0}".format('\n'.join(smb_write_locks)))
        else:
            Phase1WorkScheduler._logger.debug("No SMB Write Locks detected")
        return smb_write_locks

    @staticmethod
    def _get_phase1_source_dirs():
        phase1_source_dirs = \
            FsUtils().glob(Phase1WorkScheduler._phase1_glob_query)
        if phase1_source_dirs:
            Phase1WorkScheduler._logger.debug("Located Phase1 Source Dirs\n{0}".format('\n'.join(phase1_source_dirs)))
        else:
            Phase1WorkScheduler._logger.debug("No Phase1 Source Dirs found")
        return phase1_source_dirs

    @staticmethod
    def _get_existing_record(phase1_source_dir):
        Phase1WorkScheduler._logger.debug(
            "Attempting to retrieve record for dir:{0} from Phase1Db".format(phase1_source_dir))
        record_in_db = Phase1Db().get_work_item(phase1_source_dir)
        if record_in_db:
            Phase1WorkScheduler._logger.debug("Successfully retrieved db record")
            Phase1WorkScheduler._logger.debug("{0}".format(record_in_db))
            return record_in_db
        else:
            Phase1WorkScheduler._logger.debug("No db record found")
            return None

    @staticmethod
    def _get_new_smb_write_lock_value(phase1_source_dir, mtime, smb_write_locks, existing_record):
        if not existing_record:
            smb_write_lock_last_seen = mtime
            Phase1WorkScheduler._logger.debug("This is the first time we see {0},"
                               " so setting smb_write_lock to its mtime:{1}".format(phase1_source_dir, mtime))

        elif phase1_source_dir in smb_write_locks:
            smb_write_lock_last_seen = datetime.datetime.now()
            Phase1WorkScheduler._logger.debug("{0} has smb write lock, so setting it to now:{1}".format(
                phase1_source_dir, smb_write_lock_last_seen))
        else:
            smb_write_lock_last_seen = existing_record.last_smb_write_lock
            Phase1WorkScheduler._logger.debug(
                "{0} is an existing record that is not locked, so will keep the prior value {1}".format(
                    phase1_source_dir,
                    smb_write_lock_last_seen))

        return smb_write_lock_last_seen