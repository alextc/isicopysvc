__author__ = 'alextc'
import unittest
import random
import os
import time
import logging
from work.phase1workscheduler import Phase1WorkScheduler
from model.phase1workitem import Phase1WorkItem
from fs.fsutils import FsUtils


class Phase1WorkSchedulerTests(unittest.TestCase):

    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_must_detect_stillness(self):
        phase1_work_scheduler = Phase1WorkScheduler()
        phase1_item = self._generate_phase1_work_item()
        self.assertTrue(os.path.exists(phase1_item.phase1_source_dir))
        self.assertFalse(
            phase1_work_scheduler._is_mtime_stillness_threshold_reached(path=phase1_item.phase1_source_dir))
        time.sleep(Phase1WorkScheduler._mtime_stillness_threshold_in_sec + 1)
        self.assertTrue(
            phase1_work_scheduler._is_mtime_stillness_threshold_reached(path=phase1_item.phase1_source_dir))

    def _generate_phase1_work_item(self):
            """
            :rtype: Phase1WorkItem
            """
            phase1_source_dir_name = random.randint(10000, 900000)
            phase1_source_dir_path = \
                os.path.join(Phase1WorkSchedulerTests._root_path, str(phase1_source_dir_name))
            os.mkdir(phase1_source_dir_path)
            last_modified = FsUtils.try_to_get_dir_last_modified_time(phase1_source_dir_path)
            return Phase1WorkItem(
                source_dir=phase1_source_dir_path,
                tree_creation_time=last_modified,
                tree_last_modified=last_modified)

if __name__ == '__main__':
    format_logging = "[%(asctime)s %(process)s %(message)s"
    logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=format_logging)
    unittest.main()