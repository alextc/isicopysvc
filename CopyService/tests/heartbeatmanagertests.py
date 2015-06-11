__author__ = 'alextc'
import unittest
import random
import os
import logging
import time
from model.phase2workitem import Phase2WorkItem
from sql.heartbeatdb import HeartBeatDb
from cluster.heartbeatmanager import HeartBeatManager


class HeartBeatManagerTests(unittest.TestCase):

    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_must_write_heart_beat(self):
        # setup
        phase2_work_item = self._generate_phase2_work_item()

        # Test
        heart_beat_db = HeartBeatDb()
        heart_beat_manager = HeartBeatManager(heart_beat_db, phase2_work_item)
        heart_beat_manager.write_heart_beat()

        # validate
        confirmation = heart_beat_db.get_heart_beat(phase2_work_item.phase2_source_dir)
        self.assertEquals(confirmation, phase2_work_item)

    def test_must_not_write_heart_beat_since_not_enough_time_passed_since_the_last_one(self):
        # setup
        phase2_work_item = self._generate_phase2_work_item()

        # Test
        heart_beat_db = HeartBeatDb()
        heart_beat_manager = HeartBeatManager(heart_beat_db, phase2_work_item)
        heart_beat_manager.write_heart_beat()
        confirmation1 = heart_beat_db.get_heart_beat(phase2_work_item.phase2_source_dir)
        self.assertEquals(confirmation1, phase2_work_item)

        # Writing another heartbeat right away - should fail
        heart_beat_manager.write_heart_beat()
        confirmation2 = heart_beat_db.get_heart_beat(phase2_work_item.phase2_source_dir)
        # Nothing should have changed, since the heartbeat was not wtitten
        self.assertEquals(confirmation1, confirmation2)

    def test_must_write_heart_beat_since_enough_time_passed_since_the_last_one(self):
        # setup
        phase2_work_item = self._generate_phase2_work_item()

        # Test
        heart_beat_db = HeartBeatDb()
        heart_beat_manager = HeartBeatManager(heart_beat_db, phase2_work_item)
        heart_beat_manager.write_heart_beat()
        confirmation1 = heart_beat_db.get_heart_beat(phase2_work_item.phase2_source_dir)
        self.assertEquals(confirmation1, phase2_work_item)

        # Writing another heartbeat after sleeping longer then half the heartbeat threshold
        time.sleep((Phase2WorkItem.heart_beat_max_threshold_in_sec / 2) + 1)
        heart_beat_manager.write_heart_beat()
        confirmation2 = heart_beat_db.get_heart_beat(phase2_work_item.phase2_source_dir)
        # Nothing should have changed, since the heartbeat was not written
        print "Confirmation1\n{0}".format(confirmation1)
        print "Confirmation2\n{0}".format(confirmation2)
        self.assertNotEquals(confirmation1, confirmation2)

    def test_must_not_take_ownership_if_another_node_owns_dir_and_its_heartbeat_not_stale(self):
        # Setup
        orig_owner_phase2_work_item = self._generate_phase2_work_item()

        # Test
        heart_beat_db = HeartBeatDb()
        orig_heart_beat_manager = HeartBeatManager(heart_beat_db, orig_owner_phase2_work_item)
        orig_heart_beat_manager.write_heart_beat()
        confirmation1 = heart_beat_db.get_heart_beat(orig_owner_phase2_work_item.phase2_source_dir)
        self.assertEquals(confirmation1, orig_owner_phase2_work_item)

        # Try to claim ownership - different pid or host
        new_owner_phase2_work_item = orig_owner_phase2_work_item
        new_owner_phase2_work_item.host = "foo_host"
        new_heart_beat_manager = HeartBeatManager(heart_beat_db, new_owner_phase2_work_item)

        # Validation
        self.assertFalse(new_heart_beat_manager.try_to_take_ownership_of_heart_beating())

    def _generate_phase2_work_item(self):
        phase2_source_dir_name = random.randint(10000, 900000)
        phase2_source_dir_path = os.path.join(HeartBeatManagerTests._root_path, str(phase2_source_dir_name))
        os.mkdir(phase2_source_dir_path)
        return Phase2WorkItem(phase2_source_dir=phase2_source_dir_path, state="Init")

if __name__ == '__main__':
    log_message_format = "[%(asctime)s %(process)s: %(message)s"
    logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=log_message_format)
    unittest.main()
