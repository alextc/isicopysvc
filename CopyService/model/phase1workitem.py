__author__ = 'alextc'
import datetime
import os
from model.phase1pathcalculator import Phase1PathCalc
from common.datetimeutils import DateTimeUtils


class Phase1WorkItem(object):

    def __init__(self, source_dir, tree_creation_time, tree_last_modified, smb_write_lock_last_seen=None):
        """
        :type source_dir: str
        :type tree_creation_time: datetime.datetime
        :type tree_last_modified: datetime.datetime
        :return:
        """
        assert os.path.exists(source_dir), \
              "Unable to locate phase1_source_dir {0}".format(source_dir)

        assert tree_creation_time <= tree_last_modified, "ctime must be less or equal than mtime"

        self.phase1_source_dir = os.path.abspath(source_dir)
        self.tree_creation_time = tree_creation_time
        self.tree_last_modified = tree_last_modified

        if smb_write_lock_last_seen:
            self.last_smb_write_lock = smb_write_lock_last_seen
        else:
            self.last_smb_write_lock = tree_last_modified

        self.phase2_staging_dir = Phase1PathCalc(source_dir).get_phase2_staging_dir()

    def get_stillness_period(self):
        return DateTimeUtils.get_total_seconds_for_timedelta(datetime.datetime.now() - self.last_smb_write_lock)

    def sync_from_db(self, phase1_db):
        state_in_db = phase1_db.get_work_item(self.phase1_source_dir, self.tree_creation_time)
        assert state_in_db, "Unable to get state from Phase1 Db"
        self.last_smb_write_lock = state_in_db.last_smb_write_lock
        self.tree_last_modified = state_in_db.tree_last_modified

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.phase1_source_dir == other.phase1_source_dir and \
                   self.tree_creation_time == other.tree_creation_time and \
                   self.tree_last_modified == other.tree_last_modified and \
                   self.last_smb_write_lock == other.last_smb_write_lock

    def __str__(self):
        result = "Phase1 Source:{0}\n".format(self.phase1_source_dir) + \
                 "Phase2 Staging:{0}\n".format(self.phase2_staging_dir) + \
                 "Tree Created:{0}\n".format(
                     self.tree_creation_time.strftime("%Y-%m-%d %H:%M:%S:%f")) + \
                 "Tree Last Modified:{0}\n".format(
                     self.tree_last_modified.strftime("%Y-%m-%d %H:%M:%S:%f")) + \
                 "Last SMB Write Lock:{0}\n".format(
                     self.last_smb_write_lock.strftime("%Y-%m-%d %H:%M:%S:%f"))
        return result