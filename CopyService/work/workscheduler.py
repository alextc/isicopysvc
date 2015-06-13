__author__ = 'alextc'
import glob
import logging
import random
from model.phase2workitem import Phase2WorkItem
from sql.phase2db import Phase2Db
from cluster.heartbeatmanager import HeartBeatManager
from aop.logstartandexit import LogEntryAndExit
from fs.fsutils import FsUtils


class WorkScheduler(object):
    def __init__(self):
        self._phase1_output_path = "/ifs/zones/*/copy_svc/staging/*/*/"
        self._max_retry_count = 5
        self._max_stale_hb_time_in_seconds = 30
        self._heart_beat_db = Phase2Db()

    @LogEntryAndExit(logging.getLogger())
    def try_get_new_phase2_work_item(self):
        potential_phase2_work_item = self._get_potential_work_item()
        if not potential_phase2_work_item:
            return False

        if self._try_to_take_ownership(potential_phase2_work_item):
            logging.debug("Found and claimed new work_item\n{0}".format(potential_phase2_work_item))
            return potential_phase2_work_item

        logging.debug("Returning False could neither find nor claim new work_item")
        return False

    @LogEntryAndExit(logging.getLogger())
    def _get_potential_work_item(self):
        potential_work_inputs = glob.glob(self._phase1_output_path)
        if not potential_work_inputs:
            logging.debug("No new or stranded work: exiting")
            return

        random_dir = random.choice(potential_work_inputs)
        logging.debug("Randomly selected directory: {0}".format(random_dir))
        last_modified = FsUtils.try_to_get_dir_last_modified_time(random_dir)
        if not last_modified:
            logging.debug("Attempt to get last modified time stamp failed.")
            logging.debug("Assuming that the directory was already processed by other worker. Returning None")
            return None

        return Phase2WorkItem(random_dir, last_modified)

    @LogEntryAndExit(logging.getLogger())
    def _try_to_take_ownership(self, potential_phase2_work_item):
        logging.debug("About to attempt to take ownership on work_item:\n{0}".format(potential_phase2_work_item))
        heart_beat_manager = HeartBeatManager(self._heart_beat_db, potential_phase2_work_item)
        existing_heart_beat = heart_beat_manager.get_heart_beat()
        if existing_heart_beat and not existing_heart_beat.is_heart_beat_stale():
            logging.debug("Attempt to own dir {0} failed, since it is actively owned by {1}:{2}".format(
                existing_heart_beat.phase2_source_dir,
                existing_heart_beat.host,
                existing_heart_beat.pid))
            return False

        if existing_heart_beat and existing_heart_beat.is_heart_beat_stale():
            logging.debug("Detected stale heartbeat for {0}. Attempting to take over".format(existing_heart_beat))
            if not heart_beat_manager.try_to_remove_stale_heart_beat_record():
                logging.debug("Attempt to remove stale record for {0} failed."
                              " Assuming somebody else already done it.".format(existing_heart_beat.phase2_source_dir))
                return False

        is_attempt_success = heart_beat_manager.try_to_take_ownership_of_heart_beating()
        logging.debug("Take ownership command completed: {0}".format(is_attempt_success))

        if existing_heart_beat:
            logging.debug("The ownership was taken from a previously active item.")
            logging.debug("Prior state was {0}".format(existing_heart_beat.state))
            logging.debug("Applying prior state to the new work_item")
            potential_phase2_work_item.state = existing_heart_beat.state

        return is_attempt_success