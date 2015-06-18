__author__ = 'alextc'
import unittest
import random
import os
import time
import logging
from work.phase1workscheduler import Phase1WorkScheduler
from model.phase1workitem import Phase1WorkItem
from fs.fsutils import FsUtils
from sql.phase1db import Phase1Db


class Phase1WorkSchedulerTests(unittest.TestCase):

    _root_path = "/ifs/zones/ad1/copy_svc/to/ad2"

    def test_must_add_phase1_db_when_new_phase1_dir_is_created(self):
        phase1_db = Phase1Db()
        phase1_work_scheduler = Phase1WorkScheduler()
        phase1_item = self._generate_phase1_work_item()
        self.assertTrue(os.path.exists(phase1_item.phase1_source_dir))
        self.assertFalse(
            phase1_db.get_work_item(
                phase1_item.phase1_source_dir,
                phase1_item.tree_creation_time))

        # print "About to add_insert\n{0}".format(phase1_item)
        phase1_work_scheduler.update_phase1_db([phase1_item.phase1_source_dir, ], [])

        self.assertTrue(
            phase1_db.get_work_item(
                phase1_item.phase1_source_dir,
                phase1_item.tree_creation_time))

    def test_must_update_phase1_db_when_dir_has_smb_write_lock(self):
        phase1_db = Phase1Db()
        phase1_work_scheduler = Phase1WorkScheduler()
        phase1_item = self._generate_phase1_work_item()
        self.assertTrue(os.path.exists(phase1_item.phase1_source_dir))
        self.assertFalse(
            phase1_db.get_work_item(
                phase1_item.phase1_source_dir,
                phase1_item.tree_creation_time))
        phase1_work_scheduler.update_phase1_db([phase1_item.phase1_source_dir, ], [])

        item_at_insertion_time = phase1_db.get_work_item(
            phase1_item.phase1_source_dir,
            phase1_item.tree_creation_time)
        self.assertTrue(item_at_insertion_time)

        time.sleep(1)

        # simulating smb_write_lock by passing the sut as smb_write_locked
        print "About to update smb_write_lock on\n{0}".format(item_at_insertion_time)
        phase1_work_scheduler.update_phase1_db(
            [phase1_item.phase1_source_dir, ],
            [phase1_item.phase1_source_dir, ])

        item_post_update = phase1_db.get_work_item(
            phase1_item.phase1_source_dir,
            phase1_item.tree_creation_time)

        print "Item after smb_lock_update{0}\n".format(item_post_update)
        self.assertTrue(item_at_insertion_time.last_smb_write_lock < item_post_update.last_smb_write_lock)

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