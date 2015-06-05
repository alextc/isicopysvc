__author__ = 'alextc'
import os

class Phase2WorkItem(object):

    _copy_service_root = "/ifs/zones/"

    def __init__(self, source_dir, state, host, pid, heartbeat=None):
        self.state = state
        self.source_dir = source_dir
        self.host = host
        self.pid = pid
        self.heartbeat = heartbeat

    def get_target_dir(self):
        return  self.source_dir.replace("staging","from")

    def get_acl_template_dir(self):
        # sample source_dir /ifs/zones/ad1/copy_svc/staging/ad2/foo/

        # this will remove the trailing /
        normalized_source_dir = os.path.abspath(self.source_dir)

        # first get the path up to the zone (/ifs/zones/ad1/copy_svc/staging/ad2)
        # second split and get the last element (ad2)
        destination_zone = os.path.split(normalized_source_dir)[0].split('/')[-1]

        return Phase2WorkItem._copy_service_root + destination_zone

    def __str__(self):
        result = "State:" + self.state + "\n" + \
                 "Source:" + self.source_dir + "\n" + \
                 "Target:" + self.get_target_dir() + "\n" + \
                 "Host:" + self.host + "\n" + \
                 "ACL Template Dir:" + self.get_acl_template_dir()

        if self.heartbeat:
            result = result + "\n" + self.heartbeat.strftime("%Y-%m-%d %H:%M:%S")

        return result