__author__ = 'alextc'
import unittest
import random
import os
import shutil
from sql.writelockdb import WriteLockDb
from work.workscheduler import WorkScheduler
from model.phase2workitem import Phase2WorkItem
from fs.fsutils import FsUtils


class WorkSchedulerTests(unittest.TestCase):

    _heart_beat_db_wrapper = WriteLockDb("/ifs/copy_svc/files.db")
    _work_scheduler = WorkScheduler()
    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_must_claim_phase2_work_item(self):
        # Setup
        phase2_work_item = self._generate_phase2_work_item()

        # Test
        result = WorkSchedulerTests._work_scheduler.try_get_new_phase2_work_item()

        # Validate
        self.assertTrue(result)

        # Cleanup
        shutil.rmtree(phase2_work_item.phase2_source_dir)

    def test_must_not_claim_phase2_work_item_when_item_is_already_claimed(self):
        # Setup
        phase2_work_item = self._generate_phase2_work_item()
        result = WorkSchedulerTests._work_scheduler.try_get_new_phase2_work_item()
        self.assertTrue(result)

        # Test - Trying to claim already claimed item
        result = WorkSchedulerTests._work_scheduler.try_get_new_phase2_work_item()

        # Validate
        self.assertFalse(result)

        # Cleanup
        shutil.rmtree(phase2_work_item.phase2_source_dir)


    def test_must_not_claim_phase2_work_item_when_item_is_already_processed(self):
        # Setup
        phase2_work_item = self._generate_phase2_work_item()
        result = WorkSchedulerTests._work_scheduler.try_get_new_phase2_work_item()
        self.assertTrue(result)
        shutil.rmtree(phase2_work_item.phase2_source_dir)

        # Test - Trying to claim already processed item - item was removed from staging
        result = WorkSchedulerTests._work_scheduler.try_get_new_phase2_work_item()

        # Validate
        self.assertFalse(result)

    def _generate_phase2_work_item(self):
        """
        :rtype: Phase2WorkItem
        """
        phase2_source_dir_name = random.randint(10000, 900000)
        phase2_source_dir_path = os.path.join(WorkSchedulerTests._root_path, str(phase2_source_dir_name))
        os.mkdir(phase2_source_dir_path)
        last_modified = FsUtils.try_to_get_dir_last_modified_time(phase2_source_dir_path)
        return Phase2WorkItem(
            phase2_source_dir=phase2_source_dir_path,
            phase2_source_dir_last_modified=last_modified,
            state="Init")

if __name__ == '__main__':
    unittest.main()
