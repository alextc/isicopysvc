__author__ = 'alextc'
import unittest
import random
import os
import time
from work.phase1workscheduler import Phase1WorkScheduler


class Phase1Tests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/to/ad2"

    def test_must_move_still_dirs_to_staging(self):
        self._generate_large_number_of_directories()
        phase1_potential_work_items = Phase1WorkScheduler.update_phase1_db()
        for phase1_potential_work_item in phase1_potential_work_items:
            self.assertTrue(os.path.exists(phase1_potential_work_item.phase1_source_dir))

        time.sleep(Phase1WorkScheduler._smb_write_lock_stillness_threshold_in_sec + 1)

        for phase1_potential_work_item in phase1_potential_work_items:
            self.assertFalse(os.path.exists(phase1_potential_work_item.phase1_source_dir))

        for phase1_potential_work_item in phase1_potential_work_items:
            self.assertTrue(os.path.exists(phase1_potential_work_item.phase2_staging_dir))

    def _generate_large_number_of_directories(self):
        for i in range(50):
            dir_name = random.randint(10000, 900000)
            dir_path = os.path.join(Phase1Tests._root_path, str(dir_name))
            os.mkdir(dir_path)
            for j in range(10):
                file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
                f = open(file_name,'w+')
                f.close()

if __name__ == '__main__':
    unittest.main()
