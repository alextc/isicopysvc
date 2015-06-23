__author__ = 'alextc'
import unittest
from log.loggerfactory import LoggerFactory
from testutils.workitemsfactory import WorkItemsFactory
from testutils.cleaner import Cleaner
from phase2work.phase2worker import Phase2Worker
from phase2work.phase2workscheduler import Phase2WorkScheduler
from cluster.heartbeatmanager import HeartBeatManager
from sql.phase2db import Phase2Db


class Phase2StressTests(unittest.TestCase):
    def setUp(self):
        self._logger = LoggerFactory.create(Phase2StressTests.__name__)
        Cleaner().clean_phase2()

    def test_stress(self):
        worker = Phase2Worker()
        work_scheduler = Phase2WorkScheduler()
        heart_beat_db = Phase2Db()
        for i in range(100):
            phase2_work_item = WorkItemsFactory.create_phase2_work_item()
            my_claimed_phase2_work_item = work_scheduler.try_get_new_phase2_work_item()
            self.assertTrue(my_claimed_phase2_work_item)
            self.assertTrue(phase2_work_item.phase1_source_dir == my_claimed_phase2_work_item.phase1_source_dir)
            # state should be different
            self.assertFalse(phase2_work_item == my_claimed_phase2_work_item)
            my_claimed_phase2_work_item.state = "ReAcl"
            worker.run(my_claimed_phase2_work_item, HeartBeatManager(heart_beat_db, my_claimed_phase2_work_item))

if __name__ == '__main__':
    unittest.main()
