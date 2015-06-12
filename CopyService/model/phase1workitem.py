__author__ = 'alextc'
import datetime
import os
from model.phase1pathcalculator import Phase1PathCalc


class Phase1WorkItem(object):

    def __init__(self, phase1_source_dir, last_modified):
        """
        :type phase1_source_dir: str
        :type last_modified: datetime.datetime
        :return:
        """
        assert os.path.exists(phase1_source_dir), \
              "Unable to locate phase1_source_dir {0}".format(phase1_source_dir)

        self.phase1_source_dir = os.path.abspath(phase1_source_dir)
        self.last_modified = last_modified
        self.last_smb_lock_detected = last_modified
        self.phase2_staging_dir = Phase1PathCalc(phase1_source_dir).get_phase2_staging_dir()

    def __str__(self):
        result = "Phase1 Source:{0}\n".format(self.phase1_source_dir) + \
                 "Phase2 Staging:{0}\n".format(self.phase2_staging_dir) + \
                 "Last Modified:{0}\n".format(
                     self.last_modified.strftime("%Y-%m-%d %H:%M:%S")) + \
                 "Last SMB Write Lock:{0}\n".format(
                     self.last_smb_lock_detected.strftime("%Y-%m-%d %H:%M:%S"))
        return result