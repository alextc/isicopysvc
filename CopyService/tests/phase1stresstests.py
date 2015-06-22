__author__ = 'alextc'
import unittest
import random

from work.phase1workscheduler import Phase1WorkScheduler
from work.phase1worker import Phase1Worker
from fs.fsutils import FsUtils
from sql.phase1db import Phase1Db
from testutils.workitemsfactory import WorkItemsFactory


class Phase1StressTests(unittest.TestCase):
    _phase1_root_path = "/ifs/zones/ad1/copy_svc/to/ad2"
    _phase2_root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    _user_actions_during_test = []

    def setUp(self):
        # Starting clean
        print "Cleaning Phase1Db"
        Phase1Db().clear_work_items()
        print "Cleaning Phase1 Source Dirs"
        FsUtils.clear_dir(Phase1StressTests._phase1_root_path)
        print "Cleaning Phase2 Source Dirs"
        FsUtils.clear_dir(Phase1StressTests._phase2_root_path)

    def test_stress(self):
        phase1_worker = Phase1Worker()
        for i in range(100):
            user_action = self._select_random_user_action()
            if user_action == "new_folder":
                new_work_item = WorkItemsFactory.create_phase1_work_item()
                Phase1StressTests._user_actions_during_test.append(new_work_item)

            Phase1WorkScheduler().run()
            phase1_worker.run()
            self._validate_state()

    def _validate_state(self):
        for user_action in Phase1StressTests._user_actions_during_test:
            self.assertTrue(user_action.is_state_valid(
                stillness_threshold_in_sec=Phase1Worker._smb_write_lock_stillness_threshold_in_sec,
                phase1_db=Phase1Db()))

    def _select_random_user_action(self):
        user_actions = ['new_folder', 'delete_folder', 'smb_write_lock_file', 'noop']
        return random.choice(user_actions)

if __name__ == '__main__':
    unittest.main()