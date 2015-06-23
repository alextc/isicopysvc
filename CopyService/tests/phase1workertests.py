__author__ = 'alextc'
import unittest
import os
import time
import logging
import shutil
from phase1work.phase1workscheduler import Phase1WorkScheduler
from phase1work.phase1worker import Phase1Worker
from sql.phase1db import Phase1Db
from fs.fsutils import FsUtils
from testutils.workitemsfactory import WorkItemsFactory


class Phase1WorkerTests(unittest.TestCase):
    _phase1_source_root_path = "/ifs/zones/ad1/copy_svc/to/ad2"
    _phase2_source_root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def setUp(self):
        # Starting clean
        print "Cleaning Phase1Db"
        Phase1Db.clear_work_items()
        print "Cleaning Phase1 Source Dirs"
        FsUtils.clear_dir(Phase1WorkerTests._phase1_source_root_path)
        print "Cleaning Phase2 Source Dirs"
        FsUtils.clear_dir(Phase1WorkerTests._phase2_source_root_path)

    def test_must_get_still_items_from_db(self):
        phase1_work_item = WorkItemsFactory().create_phase1_work_item()
        Phase1WorkScheduler().run()

        time.sleep(Phase1Worker._smb_write_lock_stillness_threshold_in_sec + 1)

        still_items = Phase1Worker()._get_still_work_items_from_db()
        self.assertTrue(len(still_items) > 0)
        self.assertTrue(phase1_work_item in still_items)

        # Clean-up
        shutil.rmtree(phase1_work_item.phase1_source_dir)
        Phase1Db().remove_work_item(phase1_work_item)

    def test_must_move_still_work_items_to_phase2_staging_area(self):
        phase1_work_item = WorkItemsFactory().create_phase1_work_item()
        Phase1WorkScheduler().run()
        time.sleep(Phase1Worker._smb_write_lock_stillness_threshold_in_sec + 1)
        still_items = Phase1Worker._get_still_work_items_from_db()
        self.assertTrue(len(still_items) > 0)
        self.assertTrue(phase1_work_item in still_items)

        Phase1Worker()._move_still_items_to_staging(still_items)
        self.assertTrue(os.path.exists(phase1_work_item.phase2_staging_dir))
        self.assertFalse(os.path.exists(phase1_work_item.phase1_source_dir))

        # Clean-up
        shutil.rmtree(phase1_work_item.phase2_staging_dir)
        Phase1Db().remove_work_item(phase1_work_item)

if __name__ == '__main__':
    log_message_format = "[%(asctime)s %(process)s: %(message)s"
    logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=log_message_format)
    unittest.main()
