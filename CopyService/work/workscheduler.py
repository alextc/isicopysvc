__author__ = 'alextc'
import glob
import logging
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
            potential_phase2_work_item = Phase2WorkItem("Init", potential_work_input, socket.gethostname())
            if self.try_to_take_ownership(potential_phase2_work_item):
                logging.debug("Found new work item {0}".format(potential_work_input))
                # now that the work is claimed let's write our first heartbeat for this work item
                heart_beat_db = HeartBeatDb()
                heart_beat_db.write_heart_beat(potential_phase2_work_item)
                return potential_phase2_work_item

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

        # TODO: Check for Stranded Work
        return True

    def is_heartbeat_stale(self, directory):
        logging.debug("Entered is_heartbeat_stale")
        heart_beat_db = HeartBeatDb()
        heart_beat = heart_beat_db.get_heart_beat(directory)
        if heart_beat:
            logging.debug("Got heartbeat {0}".format(heart_beat['directory']))

        #TODO: check if heartbeat is stale
        return  False

    def try_to_take_ownership(self, potential_phase2_work_item):
        logging.debug("Entered try_to_take_ownership")
        heart_beat_db = HeartBeatDb()
        heart_beat_db.write_heart_beat(potential_phase2_work_item)

        confirmation = heart_beat_db.get_heart_beat(potential_phase2_work_item.source_dir)
        logging.debug("Received confirmation of heart beat write. {0}, {1}, {2}",
                      confirmation['directory'], confirmation['host'], confirmation['heartbeat'])
        return confirmation['host'] == socket.gethostname()