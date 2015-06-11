__author__ = 'alextc'
import glob
import logging
import os
from model.phase2workitem import Phase2WorkItem
from sql.heartbeatdb import HeartBeatDb
from cluster.heartbeatmanager import HeartBeatManager


class WorkScheduler(object):
    def __init__(self):
        self._phase1_output_path = "/ifs/zones/*/copy_svc/staging/*/*/"
        self._max_retry_count = 5
        self._max_stale_hb_time_in_seconds = 30
        self._heart_beat_db = HeartBeatDb()

    def try_get_new_phase2_work_item(self):
        potential_work_inputs = self.get_potential_work()
        # logging.debug("potential_work_inputs:\n{0}".format("\n".join(potential_work_inputs)))

        if not potential_work_inputs:
            logging.debug("try_get_new_phase2_work_item has nothing todo, exiting")
            return False

        # logging.debug("Located folders in the staging area:\n{0}".format("\n".join(potential_work_inputs)))
        for potential_work_input in potential_work_inputs:
            if not os.path.exists(potential_work_input):
                logging.debug("Directory {0} is not longer present. Assuming somebody else processed it".format(
                        potential_work_input))
                continue

            logging.debug("About to try claiming directory {0}".format(potential_work_input))
            potential_phase2_work_item = Phase2WorkItem(potential_work_input)
            if self.try_to_take_ownership(potential_phase2_work_item):
                logging.debug("Found and claimed new work_item {0}".format(potential_work_input))
                return potential_phase2_work_item

        logging.debug("Returning False could neither find or claim new work")
        return False

    def get_potential_work(self):
        potential_work_inputs = glob.glob(self._phase1_output_path)
        # logging.debug("potential_work_inputs:\n{0}".format("\n".join(potential_work_inputs)))
        if not potential_work_inputs:
            logging.debug("No new or stranded work: exiting")
            return

        logging.debug("yes there is potential work")
        return potential_work_inputs

    def try_to_take_ownership(self, potential_phase2_work_item):
        heart_beat_manager = HeartBeatManager(self._heart_beat_db, potential_phase2_work_item)
        existing_heart_beat = heart_beat_manager.get_heart_beat()
        if existing_heart_beat and not existing_heart_beat.is_heart_beat_stale():
            logging.debug("Attempt to own dir {0} failed, since it is actively owned by {1}:{2}".format(
                existing_heart_beat.phase2_source_dir,
                existing_heart_beat.heartbeat,
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
        return is_attempt_success