__author__ = 'alextc'
import unittest
import random
import os
import time
import datetime
from phase1work.phase1workscheduler import Phase1WorkScheduler
from phase1work.phase1worker import Phase1Worker
from sql.phase1db import Phase1Db
from sql.heartbeatdb import HeartBeatDb
from testutils.workitemsfactory import WorkItemsFactory
from testutils.cleaner import Cleaner
from testutils.heartbeatassertions import HeartBeatAssertions
from log.loggerfactory import LoggerFactory


class Phase1StoryTests(unittest.TestCase):

    _user_actions_during_test = []

    def setUp(self):
        self._logger = LoggerFactory().create(Phase1StoryTests.__name__)
        Cleaner().clean_phase1()
        self._heartbeatdb = HeartBeatDb("phase1")
        self._heartbeat_assertions = HeartBeatAssertions()

    def test_phase1_story(self):
        phase1_work_scheduler = Phase1WorkScheduler()
        phase1_worker = Phase1Worker()
        for i in range(100):
            user_action = self._select_random_user_action()
            self._logger.debug("User action is {0}".format(user_action))
            if user_action == "noop":
                time.sleep(0.1)
            elif user_action == "new_folder":
                self._process_new_folder_user_action(phase1_work_scheduler)
                self._heartbeat_assertions.assert_heartbeat_phase1_was_written()
            elif user_action == "smb_write_lock_file":
                self._process_smb_write_lock_user_action(phase1_work_scheduler)
            else:
                raise ValueError("Unexpected user action {0}".format(user_action))

            phase1_worker.run()
            self._assert_work_items_are_in_valid_state()

    def _process_new_folder_user_action(self, phase1_work_scheduler):
        new_work_item = WorkItemsFactory().create_phase1_work_item()
        self._logger.debug("WorkItemFactory generated new phase1_work_item:\n{0}".format(new_work_item))
        Phase1StoryTests._user_actions_during_test.append(new_work_item)
        phase1_work_scheduler.run()

    def _process_smb_write_lock_user_action(self, phase1_work_scheduler):
        # Getting a random item in Db
        # smb_write_lock operation is only makes sense on an item already in Phase1Db
        items_in_db = Phase1Db().get_all_work_items()

        # Nothing in Db so nothing to lock, exiting this case
        if not items_in_db:
            return

        work_item_to_apply_smb_write_lock = random.choice(items_in_db)
        self._logger.debug("Faking smb write lock on:\n{0}".format(work_item_to_apply_smb_write_lock))

        # Simulating Phase1 run
        phase1_source_dirs = phase1_work_scheduler._get_phase1_source_dirs()
        # Faking smb_write_lock by passing work_item_to_apply_smb_lock to _update_phase1_db
        phase1_work_scheduler._update_phase1_db(
            phase1_source_dirs,
            [work_item_to_apply_smb_write_lock.phase1_source_dir, ])

        # Syncing user_action. Need to do this so that validation works as expected
        index = Phase1StoryTests._user_actions_during_test.index(work_item_to_apply_smb_write_lock)
        Phase1StoryTests.sync_work_item_from_db(Phase1StoryTests._user_actions_during_test[index])

    @staticmethod
    def sync_work_item_from_db(phase1_work_item):
        state_in_db = Phase1Db().get_work_item(phase1_work_item.phase1_source_dir, phase1_work_item.birth_time)
        assert state_in_db, "Unable to get state from Phase1 Db"
        phase1_work_item.last_smb_write_lock = state_in_db.last_smb_write_lock
        phase1_work_item.tree_last_modified = state_in_db.tree_last_modified

    def _assert_work_items_are_in_valid_state(self):
        for user_action in Phase1StoryTests._user_actions_during_test:
            self._logger.debug("About to validate item:\n{0}".format(user_action))
            self.assertTrue(self.is_phase1_work_item_in_valid_state(user_action))

    @staticmethod
    def is_phase1_work_item_in_valid_state(phase1_work_item):
        if abs(phase1_work_item.get_stillness_period() - Phase1Worker._smb_write_lock_stillness_threshold_in_sec) <= 1:
            # This is undefined period values are too close to each other, assuming True
            return True

        if phase1_work_item.get_stillness_period() > Phase1Worker._smb_write_lock_stillness_threshold_in_sec:
            assert not os.path.exists(phase1_work_item.phase1_source_dir), \
                "Phase1 source dir should not exist after stillness threshold"
            assert os.path.exists(phase1_work_item.phase2_staging_dir), \
                "Phase2 Staging dir should exist after stillness threshold"
            assert not Phase1Db().get_work_item(
                phase1_work_item.phase1_source_dir,
                phase1_work_item.birth_time,
                validate_pre_conditions=False), \
                "Db record should not exist after stillness threshold"
        else:
            assert os.path.exists(phase1_work_item.phase1_source_dir), \
                "Phase1 source dir {0}\n should exist before stillness threshold expires.\n" \
                "Time of check {1}\n" \
                "smb_lock_last_seen {2}".format(
                    phase1_work_item.phase1_source_dir,
                    datetime.datetime.now(),
                    phase1_work_item.last_smb_write_lock)

            assert not os.path.exists(phase1_work_item.phase2_staging_dir), \
                "Phase2 Staging dir should not exist after stillness threshold"

            assert Phase1Db().get_work_item(phase1_work_item.phase1_source_dir, phase1_work_item.birth_time), \
                "Db record should have existed for dir:{0} before stillness threshold".format(
                    phase1_work_item.phase1_source_dir)

        return True

    def _select_random_user_action(self):
        # TODO: Add Delete - currently this use case is undefined
        user_actions = ['new_folder', 'smb_write_lock_file', 'noop']
        return random.choice(user_actions)

if __name__ == '__main__':
    unittest.main()