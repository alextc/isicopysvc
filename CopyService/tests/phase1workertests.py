__author__ = 'alextc'
import unittest
import random
import os
import time
import logging
import shutil
from work.phase1workscheduler import Phase1WorkScheduler
from work.phase1worker import Phase1Worker
from fs.fsutils import FsUtils
from sql.phase1db import Phase1Db
from model.phase1workitem import Phase1WorkItem


class Phase1WorkerTests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/to/ad2"
    _num_dirs_to_gen = 5

    def test_must_get_still_items_from_db(self):
        phase1_work_item = self._generate_phase1_work_item()
        Phase1WorkScheduler().run()

        time.sleep(Phase1Worker._smb_write_lock_stillness_threshold_in_sec + 1)

        still_items = Phase1Worker()._get_still_work_times_from_db()
        self.assertTrue(len(still_items) > 0)
        self.assertTrue(phase1_work_item in still_items)

        # Clean-up
        shutil.rmtree(phase1_work_item.phase1_source_dir)
        Phase1Db().remove_work_item(phase1_work_item)

    def test_must_move_still_work_items_to_phase2_staging_area(self):
        phase1_work_item = self._generate_phase1_work_item()
        Phase1WorkScheduler().run()
        time.sleep(Phase1Worker._smb_write_lock_stillness_threshold_in_sec + 1)
        still_items = Phase1Worker._get_still_work_times_from_db()
        self.assertTrue(len(still_items) > 0)
        self.assertTrue(phase1_work_item in still_items)

        Phase1Worker()._move_still_items_to_staging(still_items)
        self.assertTrue(os.path.exists(phase1_work_item.phase2_staging_dir))
        self.assertFalse(os.path.exists(phase1_work_item.phase1_source_dir))

        # Clean-up
        shutil.rmtree(phase1_work_item.phase2_staging_dir)
        Phase1Db().remove_work_item(phase1_work_item)

    def _generate_phase1_work_item(self):
            """
            :rtype: Phase1WorkItem
            """
            phase1_source_dir_name = random.randint(10000, 900000)
            phase1_source_dir_path = \
                os.path.join(Phase1WorkerTests._root_path, str(phase1_source_dir_name))
            os.mkdir(phase1_source_dir_path)
            self.assertTrue(os.path.exists(phase1_source_dir_path))
            last_modified = FsUtils.try_to_get_dir_last_modified_time(phase1_source_dir_path)
            self.assertFalse(Phase1Db().get_work_item(
                phase1_source_dir_path,
                last_modified))

            return Phase1WorkItem(
                source_dir=phase1_source_dir_path,
                tree_creation_time=last_modified,
                tree_last_modified=last_modified)

if __name__ == '__main__':
    log_message_format = "[%(asctime)s %(process)s: %(message)s"
    logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=log_message_format)
    unittest.main()
