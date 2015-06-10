__author__ = 'alextc'
import unittest
import random
import os
import logging

from model.phase2workitem import Phase2WorkItem
from sql.heartbeatdb import HeartBeatDb
from cluster.heartbeatmanager import HeartBeatManager

class HeartBeatManagerTests(unittest.TestCase):

    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_must_write_heart_beat(self):

        log_message_format = "[%(asctime)s %(process)s: %(message)s"
        logging.basicConfig(filename='/ifs/copy_svc/wip.log',level=logging.DEBUG, format=log_message_format)

        # setup
        phase2_source_dir_name = random.randint(10000, 900000)
        phase2_source_dir_path = os.path.join(HeartBeatManagerTests._root_path, str(phase2_source_dir_name))
        os.mkdir(phase2_source_dir_path)
        phase2_work_item = Phase2WorkItem(phase2_source_dir=phase2_source_dir_path, state="Init")

        # Test
        heart_beat_db = HeartBeatDb()
        heart_beat_manager = HeartBeatManager(heart_beat_db, phase2_work_item)
        heart_beat_manager.write_heart_beat()

        # validate
        confirmation = heart_beat_db.get_heart_beat(phase2_work_item.phase2_source_dir)
        self.assertEquals(confirmation.phase2_source_dir, phase2_work_item.phase2_source_dir)

if __name__ == '__main__':
    unittest.main()

