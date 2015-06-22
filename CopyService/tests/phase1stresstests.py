__author__ = 'alextc'
import unittest
import random
from work.phase1workscheduler import Phase1WorkScheduler
from work.phase1worker import Phase1Worker
from sql.phase1db import Phase1Db
from testutils.workitemsfactory import WorkItemsFactory
from testutils.cleaner import Cleaner


class Phase1StressTests(unittest.TestCase):

    _user_actions_during_test = []

    def setUp(self):
        Cleaner().clean_phase1()

    def test_stress(self):
        phase1_work_scheduler = Phase1WorkScheduler()
        phase1_worker = Phase1Worker()
        for i in range(100):
            user_action = self._select_random_user_action()

            if user_action == "noop":
                continue
            elif user_action == "smb_write_lock_file":
                # smb_write_lock operation is only makes sense on an item already in Phase1Db
                items_in_db = Phase1Db().get_all_work_items()

                # Nothing in Db so nothing to lock exiting this case
                if not items_in_db:
                    continue

                work_item_to_apply_smb_write_lock = random.choice(items_in_db)
                phase1_source_dirs = phase1_work_scheduler._get_phase1_source_dirs()
                # Faking smb_write_lock
                phase1_work_scheduler._update_phase1_db(
                    phase1_source_dirs,
                    [work_item_to_apply_smb_write_lock.phase1_source_dir, ])

                # Syncing user_action
                index = Phase1StressTests._user_actions_during_test.index(work_item_to_apply_smb_write_lock)
                Phase1StressTests._user_actions_during_test[index].sync_from_db(Phase1Db())
            elif user_action == "new_folder":
                new_work_item = WorkItemsFactory.create_phase1_work_item()
                Phase1StressTests._user_actions_during_test.append(new_work_item)
                phase1_work_scheduler.run()
            else:
                raise ValueError("Unexpected user action {0}".format(user_action))

            phase1_worker.run()
            self._validate_state()

    def _validate_state(self):
        for user_action in Phase1StressTests._user_actions_during_test:
            self.assertTrue(user_action.is_state_valid(
                stillness_threshold_in_sec=Phase1Worker._smb_write_lock_stillness_threshold_in_sec,
                phase1_db=Phase1Db()))

    def _select_random_user_action(self):
        # TODO: Add Delete - currently this use case is undefined
        user_actions = ['new_folder', 'smb_write_lock_file', 'noop']
        return random.choice(user_actions)

if __name__ == '__main__':
    unittest.main()