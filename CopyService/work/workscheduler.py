__author__ = 'alextc'
import glob
import logging
import os
import socket
from model.phase2workitem import Phase2WorkItem
from sql.heartbeatdb import HeartBeatDb

class WorkScheduler(object):
    def __init__(self):
        self._phase1_output_path = "/ifs/zones/*/copy_svc/staging/*/*/"
        self._max_retry_count = 5
        self._max_stale_hb_time_in_seconds = 30

    def try_get_new_phase2_work_item(self):
        logging.debug("Entered try_get_new_phase2_work_item")
        if not self.is_there_any_work():
            return False

        potential_work_inputs = glob.glob(self._phase1_output_path)
        for potential_work_input in potential_work_inputs:
            if self.try_to_take_ownership(potential_work_input):
                phase2_work_item = Phase2WorkItem("Init", potential_work_input)
                logging.debug("Found new work item {0}".format(phase2_work_item))
                # now that the work is claimed let's write our first heartbeat for this work item
                heart_beat_db = HeartBeatDb()
                heart_beat_db.write_heart_beat(phase2_work_item.source_dir, socket.gethostname())
                return phase2_work_item

        return False

    def try_to_get_stranded_work(self):
        logging.debug("Entered try_to_get_stranded_work")
        if not self.is_there_any_work():
            return False

        potential_stranded_work_inputs = glob.glob(self._phase1_output_path)
        filter(lambda x: x.endswith("_in_process"), potential_stranded_work_inputs)
        logging.debug("Located stranded folders:\n{0}".format("\n".join(potential_stranded_work_inputs)))

        for potential_strand_target in potential_stranded_work_inputs:
            if self.is_heartbeat_stale(potential_strand_target):
                work_item = self.try_to_take_ownership(self.get_original_source(potential_strand_target))
                if work_item:
                    heart_beat_db = HeartBeatDb()
                    heart_beat_db.write_heart_beat(work_item.source_dir, socket.gethostname())
                    logging.debug("returning {0}", work_item)
                    return work_item
            else:
                return False

    def is_there_any_work(self):
        logging.debug("Entered is_there_any_work")
        potential_work_inputs = glob.glob(self._phase1_output_path)
        if not potential_work_inputs:
            logging.debug("No new or stranded work: exiting")
            return False

        # TODO: try applying filter first; what happens is the list if empty to start with
        filter(lambda x: x.endswith("_in_process"), potential_work_inputs)
        if not potential_work_inputs:
            logging.debug("No new work: exiting")
            return False

        return True


    def is_heartbeat_stale(self, directory):
        logging.debug("Entered is_heartbeat_stale")
        heart_beat_db = HeartBeatDb()
        heart_beat = heart_beat_db.get_heart_beat(directory)
        logging.debug("Got heartbeat {0}".format(heart_beat))

    def try_to_take_ownership(self, potential_work_target):
        if os.path.exists(potential_work_target):
            logging.debug("Unable to locate source of work item - assuming somebody beat me to it."
                          "Exiting. Returning False.")
            return False

        try:
            os.rename(potential_work_target, potential_work_target + "_in_process")
        except IOError as e:
            logging.debug("Failed to rename {0} to _in_process. Assuming somebody beat me to it. Exiting "
                          "Returning false".format(potential_work_target))
            return False

        return True