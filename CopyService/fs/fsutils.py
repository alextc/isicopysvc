__author__ = 'alextc'
import glob

class FsUtils(object):

    def get_source_directories(self, root_path):
        return glob.glob(root_path)