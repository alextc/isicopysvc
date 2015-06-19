__author__ = 'alextc'
import unittest
import random
import os
from work.phase1workscheduler import Phase1WorkScheduler
from work.phase1worker import Phase1Worker
from fs.fsutils import FsUtils
from sql.phase1db import Phase1Db
from model.phase1workitem import Phase1WorkItem


class Phase1StressTests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/to/ad2"
    _num_dirs_to_gen = 5
    _user_actions_during_test = []

    def test_stress(self):
        phase1_worker = Phase1Worker()
        for i in range(100):
            user_action = self._select_random_user_action()
            if user_action == "new_folder":
                new_work_item = self._generate_phase1_work_item()
                print "User generated new folder\n{0}".format(new_work_item)
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

    def _generate_phase1_work_item(self):
            """
            :rtype: Phase1WorkItem
            """
            phase1_source_dir_name = random.randint(100, 9000000)
            phase1_source_dir_path = \
                os.path.join(Phase1StressTests._root_path, str(phase1_source_dir_name))
            assert not os.path.exists(phase1_source_dir_path), "Duplicate directory created by generator"
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
    unittest.main()