__author__ = 'alextc'
import glob
import logging
import os
import fcntl
import datetime
import time
import sys
import socket
from model.workstate import WorkState

class WorkScheduler(object):
    def __init__(self):
        self._work_in_progress_path = "/ifs/zones/*/copy_svc/staging/*/*/"
        self._max_retry_count = 5
        self._datetime_format = '%Y, %m, %d, %H, %M, %S, %f'
        self._max_stale_hb_time_in_seconds = 30

    def get_stranded_work(self):
        logging.debug("Entered get_stranded_work")
        potential_stranded_targets = glob.glob(self._work_in_progress_path)
        if not potential_stranded_targets:
            logging.debug("Nothing in work_in_progress_path; exiting")
            return

        filter(lambda x: x.endswith("_in_process"), potential_stranded_targets)
        if not potential_stranded_targets:
            logging.debug("Nothing in work_in_progress_path that ends with in_process; exiting")

        logging.debug("Located stranded folders:\n{0}".format("\n".join(potential_stranded_targets)))

        for potential_strand_target in potential_stranded_targets:
            if self.is_heartbeat_stale(potential_strand_target):
                self.take_ownership(self.get_original_source(potential_strand_target), True)
                state = self.get_state(potential_strand_target)
                logging.debug("returning {0}", state)
                return state

    def is_heartbeat_stale(self, ownership_path):
        hb_file = ownership_path + "/hb.dat"
        if not os.path.exists(hb_file):
            raise ValueError("Request to check heart beat on a non-existing hb.dat")

        for i in range(self._max_retry_count):
            try:
                with open(hb_file) as last_heartbeat:
                    fcntl.flock(last_heartbeat.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    last_hb_time = last_heartbeat.readline().strip()
                    if last_hb_time:
                        last_hb_datetime = datetime.datetime.strptime(last_hb_time, self._datetime_format)
                        return (datetime.datetime.utcnow() - last_hb_datetime).seconds > self._max_stale_hb_time_in_seconds
                time.sleep(1)
            except IOError:
                logging.debug(sys.exc_info()[0])

        if i == (self._max_retry_count - 1):
            raise ValueError("Unable to check heartbeat")

    def take_ownership(self, potential_work_target, ignore_prev_owner):
        expected_target_staging_dir = potential_work_target + "_in_process"

        if not ignore_prev_owner and os.path.exists(expected_target_staging_dir):
            # Since path exists and the caller wants to respect prior ownership -> Exit
            return
        elif not os.path.exists(expected_target_staging_dir):
            os.mkdir(expected_target_staging_dir)
            self._write_ownership_markers(potential_work_target)
        else:
            self._write_ownership_markers(potential_work_target)

        state = WorkState(state="Init", source_dir=potential_work_target, process_dir=expected_target_staging_dir)
        return state

    def write_ownership_markers(self, potential_work_target):
        expected_target_staging_dir = potential_work_target + "_in_process"
        expected_hb_file = expected_target_staging_dir + "/hb.dat"
        expected_source_file = expected_target_staging_dir + "/source.dat"
        expected_owner_file = expected_target_staging_dir + "/owner_data.dat"

        for i in range(self._max_retry_count):
            try:
                with open(expected_owner_file, 'w+') as owner_file:
                    fcntl.flock(expected_owner_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    owner_file.writelines(socket.gethostname() + ":" + str(os.getpid()))
                with open(expected_hb_file, 'w+') as hb_file:
                    fcntl.flock(hb_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    hb_file.writelines(datetime.datetime.utcnow().strftime(self.datetime_format_string))
                with open(expected_source_file, 'w+') as source_file:
                    fcntl.flock(source_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    source_file.writelines(potential_work_target)
            except IOError:
                logging.debug(sys.exc_info()[0])
                time.sleep(1)

        if i == (self._max_retry_count - 1):
            raise ValueError("Unable to write ownership markers")

    def get_original_source(self, ownership_path):
        original_source = ownership_path + "/source.dat"
        if os.path.exists(original_source):
            with open(original_source) as orig_source:
                my_ret = orig_source.readline().strip()
                return my_ret

        raise ValueError("Unable to get orignal source path")

    def get_state(self, ownership_path):
        if not os.path.exists(ownership_path):
            raise ValueError("Request to check state on a state.dat failed")

        state_file_name = ownership_path + "/state.dat"
        with open(state_file_name) as state_file:
            my_ret = state_file.readline()
            if not my_ret:
                raise ValueError("No state found")
            return my_ret