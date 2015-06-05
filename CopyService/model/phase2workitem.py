__author__ = 'alextc'
import os
import socket

class Phase2WorkItem(object):
    _copy_service_root = "/ifs/zones/"
    _states = ["Init", "CopyOrig", "ReAcl", "Move", "Cleanup"]

    def __init__(self, source_dir, state, host, pid, heartbeat=None):

        assert os.path.exists(Phase2WorkItem._copy_service_root), \
            "Unable to locate the root of CopyService {0}".format(Phase2WorkItem._copy_service_root)

        assert os.path.exists(os.path.abspath(source_dir)), \
            "Unable to locate Phase2WorkItem's source_dir {0}".format(source_dir)

        assert os.getpid() == pid, \
            "PID provided {0} does not match the PID of the current process".format(pid)

        assert socket.gethostname() == host, \
            "Host name does not match the name of the current host"

        assert state in Phase2WorkItem._states, \
            "Unexpected state {0} provided".format(state)

        self.state = state
        self.source_dir = os.path.abspath(source_dir)
        self.target_dir = self.source_dir.replace("staging","from")

        # first get the path up to the zone (/ifs/zones/ad1/copy_svc/staging/ad2)
        # second split and get the last element (ad2)
        self.acl_template_dir = \
            os.path.join(
                Phase2WorkItem._copy_service_root,
                os.path.split(self.source_dir)[0].split('/')[-1])

        self.host = host
        self.pid = pid
        self.heartbeat = heartbeat

    def __str__(self):
        result = "State:" + self.state + "\n" + \
                 "Source:" + self.source_dir + "\n" + \
                 "Target:" + self.target_dir + "\n" + \
                 "Host:" + self.host + "\n" + \
                 "PID:" + str(self.pid) + "\n" + \
                 "ACL Template Dir:" + self.acl_template_dir

        if self.heartbeat:
            result = result + "\n" + self.heartbeat.strftime("%Y-%m-%d %H:%M:%S")

        return result
