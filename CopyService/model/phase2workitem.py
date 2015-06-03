__author__ = 'alextc'
class Phase2WorkItem(object):
    def __init__(self, source_dir, state, host, heartbeat=None):
        self.state = state
        self.source_dir = source_dir
        self.host = host
        self.heartbeat = heartbeat

    def get_target_dir(self):
        return  self.source_dir.replace("staging","from")

    def __str__(self):
        result = "State:" + self.state + "\n" + \
                "Source:" + self.source_dir + "\n" + \
                "Target:" + self.get_target_dir() + "\n" + \
                "Host:" + self.host

        if self.heartbeat:
            result = result + "\n" + self.heartbeat

        return  result