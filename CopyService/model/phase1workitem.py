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

    def is_state_valid(self, stillness_threshold_in_sec, phase1_db):

        if abs(self.get_stillness_period() - stillness_threshold_in_sec) <= 1:
            # This is undefined period values are too close to each other, assuming True
            return True

        if self.get_stillness_period() > stillness_threshold_in_sec:
            assert not os.path.exists(self.phase1_source_dir), \
                "Phase1 source dir should not exist after stillness threshold"
            assert os.path.exists(self.phase2_staging_dir), \
                "Phase2 Staging dir should exist after stillness threshold"
            assert not phase1_db.get_work_item(self.phase1_source_dir, self.tree_creation_time), \
                "Db record should not exist after stillness threshold"
        else:
            assert os.path.exists(self.phase1_source_dir), \
                "Phase1 source dir {0}\n should exist before stillness threshold expires.\n" \
                "Time of check {1}\n" \
                "smb_lock_last_seen {2}".format(
                    self.phase1_source_dir,
                    datetime.datetime.now(),
                    self.last_smb_write_lock)
            assert not os.path.exists(self.phase2_staging_dir), \
                "Phase2 Staging dir should not exist after stillness threshold"
            assert phase1_db.get_work_item(self.phase1_source_dir, self.tree_creation_time), \
                "Db record should exist before stillness threshold"

        return True

    def sync_from_db(self, phase1_db):
        state_in_db = phase1_db().get_work_item(self.phase1_source_dir, self.tree_creation_time)
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
                     self.tree_creation_time.strftime("%Y-%m-%d %H:%M:%S")) + \
                 "Tree Last Modified:{0}\n".format(
                     self.tree_last_modified.strftime("%Y-%m-%d %H:%M:%S")) + \
                 "Last SMB Write Lock:{0}\n".format(
                     self.last_smb_write_lock.strftime("%Y-%m-%d %H:%M:%S"))
        return result