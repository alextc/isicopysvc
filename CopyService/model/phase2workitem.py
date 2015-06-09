__author__ = 'alextc'
import os
import socket
import datetime
from model.phase2pathcalculator import Phase2PathCalculator

class Phase2WorkItem(object):
    _states = ["Init", "CopyOrig", "ReAcl", "Move", "Cleanup"]

    def __init__(self, phase2_source_dir, state="Init", heartbeat=None):

        assert os.path.exists(phase2_source_dir), \
            "Unable to locate phase2_source_dir {0}".format(phase2_source_dir)
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
