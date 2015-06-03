__author__ = 'alextc'
class Phase2WorkItem(object):
    def __init__(self, state, source_dir):
        self.state = state
        self.source_dir = source_dir

    def get_target_dir(self):
        return  self._source_dir.replace("staging","from")

    def __str__(self):
        return "State:" + self.state + "\n" + \
               "Source:" + self.source_dir + "\n" + \
               "Target" + self.get_target_dir()