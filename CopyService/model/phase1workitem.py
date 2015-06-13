__author__ = 'alextc'
import datetime
import os
from model.phase1pathcalculator import Phase1PathCalc


class Phase1WorkItem(object):

    def __init__(self, source_dir, dir_creation_time, dir_last_modified):
        """
        :type source_dir: str
        :type dir_creation_time: datetime.datetime
        :type dir_last_modified: datetime.datetime
        :return:
        """
        assert os.path.exists(source_dir), \
              "Unable to locate phase1_source_dir {0}".format(source_dir)

        self.phase1_source_dir = os.path.abspath(source_dir)
        self.dir_creation_time = dir_creation_time
        self.dir_last_modified = dir_last_modified
        self.last_smb_write_lock = dir_last_modified
        self.phase2_staging_dir = Phase1PathCalc(source_dir).get_phase2_staging_dir()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.phase1_source_dir == other.phase1_source_dir and \
                   self.dir_creation_time == other.dir_creation_time and \
                   self.dir_last_modified == other.dir_last_modified and \
                   self.last_smb_write_lock == other.last_smb_write_lock

    def __str__(self):
        result = "Phase1 Source:{0}\n".format(self.phase1_source_dir) + \
                 "Phase2 Staging:{0}\n".format(self.phase2_staging_dir) + \
                 "Created:{0}\n".format(
                     self.dir_creation_time.strftime("%Y-%m-%d %H:%M:%S")) + \
                 "Last Modified:{0}\n".format(
                     self.dir_last_modified.strftime("%Y-%m-%d %H:%M:%S")) + \
                 "Last SMB Write Lock:{0}\n".format(
                     self.last_smb_write_lock.strftime("%Y-%m-%d %H:%M:%S"))
        return result