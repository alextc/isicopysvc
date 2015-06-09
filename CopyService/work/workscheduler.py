__author__ = 'alextc'
import glob
import logging
import socket
import os
from model.phase2workitem import Phase2WorkItem
from sql.heartbeatdb import HeartBeatDb

class WorkScheduler(object):
    def __init__(self):
        self._phase1_output_path = "/ifs/zones/*/copy_svc/staging/*/*/"
        self._max_retry_count = 5
        self._max_stale_hb_time_in_seconds = 30

    def try_get_new_phase2_work_item(self):
        potential_work_inputs = self.get_potential_work()
        # logging.debug("potential_work_inputs:\n{0}".format("\n".join(potential_work_inputs)))

        if not potential_work_inputs:
            logging.debug("try_get_new_phase2_work_item has nothing todo, exiting")
            return False

        # logging.debug("Located folders in the staging area:\n{0}".format("\n".join(potential_work_inputs)))
        for potential_work_input in potential_work_inputs:
            logging.debug("About to claim directory {0}".format(potential_work_input))
            potential_phase2_work_item = Phase2WorkItem(potential_work_input, "Init")
            if self.try_to_take_ownership(potential_phase2_work_item):
                logging.debug("Found and claimed new work_item {0}".format(potential_work_input))
                # now that the work is claimed let's write our first heartbeat for this work item
                heart_beat_db = HeartBeatDb()
                heart_beat_db.write_heart_beat(potential_phase2_work_item)
                # logging.debug("Returning new phase2 work item{0}".format(potential_phase2_work_item))
                # logging.debug("Returning new phase2 work item{0}".format(potential_phase2_work_item.source_dir))
                return potential_phase2_work_item

        logging.debug("Returning new False could not find or claim new work")
        return False

    def try_to_get_stranded_work(self):
        if not self.get_potential_work():
            return False

        potential_stranded_work_inputs = glob.glob(self._phase1_output_path)
        filter(lambda x: x.endswith("_in_process"), potential_stranded_work_inputs)
        # logging.debug("Located stranded folders:\n{0}".format("\n".join(potential_stranded_work_inputs)))

        for potential_strand_target in potential_stranded_work_inputs:
            if self.is_heartbeat_stale(potential_strand_target):
                work_item = self.try_to_take_ownership(self.get_original_source(potential_strand_target))
                if work_item:
                    heart_beat_db = HeartBeatDb()
                    heart_beat_db.write_heart_beat(work_item.source_dir, socket.gethostname())
                    # logging.debug("returning {0}", work_item)
                    return work_item
            else:
                return False

    def get_potential_work(self):
        potential_work_inputs = glob.glob(self._phase1_output_path)
        # logging.debug("potential_work_inputs:\n{0}".format("\n".join(potential_work_inputs)))
        if not potential_work_inputs:
            logging.debug("No new or stranded work: exiting")
            return

        # TODO: Check for Stranded Work
        logging.debug("yes there is potential work")
        return potential_work_inputs

    def is_heartbeat_stale(self, directory):
        heart_beat_db = HeartBeatDb()
        heart_beat = heart_beat_db.get_heart_beat(directory)
        #TODO: check if heartbeat is stale
        return False

    def try_to_take_ownership(self, potential_phase2_work_item):
        heart_beat_db = HeartBeatDb()
        is_attempt_success =  heart_beat_db.try_to_take_ownership(potential_phase2_work_item)
        # TODO: Add logic to detect stale hearbeat and force membership change

        logging.debug("Take ownership command completed: {0}".format(is_attempt_success))
        return is_attempt_success