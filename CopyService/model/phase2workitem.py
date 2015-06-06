__author__ = 'alextc'
import os
import socket

class Phase2WorkItem(object):
    _copy_service_root = "/ifs/zones/"
    _copy_service_from_sub_path = "/copy_svc/from/"
    _copy_service_to_sub_path = "/copy_svc/to/"
    _states = ["Init", "CopyOrig", "ReAcl", "Move", "Cleanup"]

    def __init__(self, phase2_source_dir, state, host, pid, heartbeat=None):

        assert os.path.exists(Phase2WorkItem._copy_service_root), \
            "Unable to locate the root of CopyService {0}".format(Phase2WorkItem._copy_service_root)

        assert os.path.exists(os.path.abspath(phase2_source_dir)), \
            "Unable to locate Phase2WorkItem's source_dir {0}".format(phase2_source_dir)

        assert os.getpid() == pid, \
            "PID provided {0} does not match the PID of the current process".format(pid)

        assert socket.gethostname() == host, \
            "Host name does not match the name of the current host"

        assert state in Phase2WorkItem._states, \
            "Unexpected state {0} provided".format(state)

        self.state = state
        self.phase2_source_dir = os.path.abspath(phase2_source_dir)

        # Example of phase2_source_dir /ifs/zones/ad1/copy_svc/staging/ad2/foobar
        #                               1     2   3       4      5      6    7
        # split func returns one more than the number of items, see below
        # https://docs.python.org/2/library/string.html

        self.from_zone = self.phase2_source_dir.split('/')[3]
        self.to_zone = self.phase2_source_dir.split('/')[6]

        # this is just the directory name - no path
        self.dir_name = os.path.split(self.phase2_source_dir)[1]

        os.path.split(self.phase2_source_dir)[1]

        self.phase1_source_dir = \
            Phase2WorkItem._copy_service_root + \
            self.from_zone + \
            Phase2WorkItem._copy_service_to_sub_path + \
            self.to_zone + "/" + \
            self.dir_name

        self.target_dir = \
            Phase2WorkItem._copy_service_root + \
            self.to_zone + \
            Phase2WorkItem._copy_service_from_sub_path + \
            self.from_zone + "/" + \
            self.dir_name

        # simply root of the target
        self.acl_template_dir = os.path.split(self.target_dir)[0]

        self.host = host
        self.pid = pid
        self.heartbeat = heartbeat

    def __str__(self):
        result = "State:" + self.state + "\n" + \
                 "From Zone:" + self.from_zone + "\n" + \
                 "To Zone:" + self.to_zone + "\n" + \
                 "Phase1 Source:" + self.phase1_source_dir + "\n" + \
                 "Phase2 Source:" + self.phase2_source_dir + "\n" + \
                 "Target:" + self.target_dir + "\n" + \
                 "Host:" + self.host + "\n" + \
                 "PID:" + str(self.pid) + "\n" + \
                 "ACL Template Dir:" + self.acl_template_dir

        if self.heartbeat:
            result = result + "\n" + self.heartbeat.strftime("%Y-%m-%d %H:%M:%S")

        return result
