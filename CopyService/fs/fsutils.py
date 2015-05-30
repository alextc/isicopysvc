__author__ = 'alextc'
import glob
import logging

class FsUtils(object):

    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def get_source_directories(self, root_path):
        self._logger.debug("\n\tENTERING get_source_directories")
        self._logger.debug("\n\tPARAMETER root_path\n\t\t%s", root_path)
        result = glob.glob(root_path)
        self._logger.debug("\n\tRETURNING:\n\t\t%s", "\n\t\t".join(result))
        return result
