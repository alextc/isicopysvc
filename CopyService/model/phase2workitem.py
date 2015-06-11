__author__ = 'alextc'
import os
import socket
import datetime
from model.phase2pathcalculator import Phase2PathCalculator
from common.datetimeutils import DateTimeUtils


class Phase2WorkItem(object):
    _states = ["Init", "CopyOrig", "ReAcl", "Move", "Cleanup", "Completed"]
    heart_beat_max_threshold_in_sec = 60

    def __init__(self, phase2_source_dir, state="Init", heartbeat=None):

        # Removing this Assert - in a multi threaded scenario it is quite possible to this directory not to exist
        # by the time this code executes - some other process already completed work and deleted it
        # The situation should resolve itself when an attempt is made to claim this directory.
        # at that point the directory is claimed by somebody else.
        # However, what if the directory is already processed by that time and no longer in sql???
        # assert os.path.exists(phase2_source_dir), \
        #    "Unable to locate phase2_source_dir {0}".format(phase2_source_dir)
        self.phase2_source_dir = os.path.abspath(phase2_source_dir)

        phase2_path_calculator = Phase2PathCalculator(self.phase2_source_dir)
        self.phase1_source_dir = phase2_path_calculator.get_phase1_source_dir()
        self.acl_template_dir = phase2_path_calculator.get_acl_template_path()
        self.target_dir = phase2_path_calculator.get_phase2_target_dir()
        self.state = state
        self.host = socket.gethostname()
        self.pid = os.getpid()
        if not heartbeat:
            self.heartbeat = datetime.datetime.now()
        else:
            self.heartbeat = heartbeat

    def is_heart_beat_stale(self):
        result = DateTimeUtils.get_total_seconds_for_timedelta(datetime.datetime.now() - self.heartbeat) > \
                 Phase2WorkItem.heart_beat_max_threshold_in_sec
        return result

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.phase2_source_dir == other.phase2_source_dir and \
                   self.state == other.state and \
                   self.heartbeat == other.heartbeat

    def __str__(self):
        result = "State:" + self.state + "\n" + \
                 "Phase1 Source:" + self.phase1_source_dir + "\n" + \
                 "Phase2 Source:" + self.phase2_source_dir + "\n" + \
                 "Target:" + self.target_dir + "\n" + \
                 "Host:" + self.host + "\n" + \
                 "PID:" + str(self.pid) + "\n" + \
                 "ACL Template Dir:" + self.acl_template_dir

        if self.heartbeat:
            result = result + "\n" + self.heartbeat.strftime("%Y-%m-%d %H:%M:%S")

        return result
