__author__ = 'alextc'
class WorkState(object):
    def __init__(self, state, source_dir, process_dir):
        self.state = state
        self.source_dir = source_dir
        self.target_dir = source_dir.replace("staging", "from")
        self.process_dir = process_dir

    def __str__(self):
        return "State:" + self.state + "\n" + \
               "Source:" + self.source_dir + "\n" + \
               "Target" + self.target_dir + "\n" + \
               "ProcessDir:" + self.process_dir