__author__ = 'alextc'
import glob
import logging

class FsUtils(object):

    def get_source_directories(self, root_path):
        logging.debug("FsUtils.get_source_directories ENTERING")
        logging.debug("\tPARAMETER root_path %s", root_path)
        result = glob.glob(root_path)
        logging.debug("\tRETURN:\n\t\t%s", "\n\t\t".join(result))
        return result