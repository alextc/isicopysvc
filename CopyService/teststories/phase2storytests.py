__author__ = 'alextc'
import unittest
from log.loggerfactory import LoggerFactory
from testutils.workitemsfactory import WorkItemsFactory
from testutils.cleaner import Cleaner
from phase2work.phase2worker import Phase2Worker
from phase2work.phase2workscheduler import Phase2WorkScheduler
from cluster.phase2workitemheartbeatmanager import Phase2WorkItemHeartBeatManager
from sql.phase2db import Phase2Db
from testutils.heartbeatassertions import HeartBeatAssertions
from testutils.aclassertions import AclAssertions


class Phase2StoryTests(unittest.TestCase):

    def setUp(self):
        self._logger = LoggerFactory().create(Phase2StoryTests.__name__)
        Cleaner().clean_phase2()
        self._heartbeat_assertions = HeartBeatAssertions()
        self._aclAssertions = AclAssertions()

    def test_phase2_story(self):
        worker = Phase2Worker()
        work_scheduler = Phase2WorkScheduler()
        heart_beat_db = Phase2Db()
        for i in range(100):
            phase2_work_item = WorkItemsFactory().create_phase2_work_item()
            my_claimed_phase2_work_item = work_scheduler.try_get_new_phase2_work_item()
            self._heartbeat_assertions.assert_heartbeat_phase2_was_written()
            self.assertTrue(my_claimed_phase2_work_item)
            self.assertTrue(phase2_work_item.phase1_source_dir == my_claimed_phase2_work_item.phase1_source_dir)
            # state should be different
            self.assertFalse(phase2_work_item == my_claimed_phase2_work_item)
            my_claimed_phase2_work_item.state = "ReAcl"
            worker.run(
                my_claimed_phase2_work_item,
                Phase2WorkItemHeartBeatManager(
                    heart_beat_db, my_claimed_phase2_work_item))

            self._aclAssertions.assert_acl_applied(tree=my_claimed_phase2_work_item.target_dir)

if __name__ == '__main__':
    unittest.main()
