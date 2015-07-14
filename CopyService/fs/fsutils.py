__author__ = 'alextc'
import glob
import os
import datetime
import shutil
from isiapi.getaclcommand import GetAclCommand
from isiapi.setaclcommand import SetAclCommand
from cluster.heartbeatmanager import HeartBeatManager
from log.loggerfactory import LoggerFactory
from lnx.stat import Stat


class FsUtils(object):

    _logger = LoggerFactory().create("FsUtils")

    def __init__(self):
        pass

    @staticmethod
    def glob(root_path):
        FsUtils._logger.debug("\n\tPARAMETER root_path\n\t\t%s", root_path)
        result = glob.glob(root_path)
        result_to_abs_path = [os.path.abspath(d) for d in result]
        FsUtils._logger.debug("\n\tRETURNING:\n\t\t%s", "\n\t\t".join(result_to_abs_path))
        return result_to_abs_path

    @staticmethod
    def get_dir_birth_datetime(path):
        assert os.path.exists(path), "dir:{0}, passed that was passed to dir_birth_datetime does not exist".format(path)
        return Stat.get_birth_time(path)

    """
    # ctime does not equal creation time; it changes as files are added and removed from the dir
    # Don't use to uniquely identify directories
    def try_to_get_dir_created_time(self, dir_name):

        try:
            t = os.path.getctime(dir_name)
            datetime_with_microseconds = datetime.datetime.fromtimestamp(t)
            return datetime_with_microseconds
            # return DateTimeUtils().strip_microseconds(datetime_with_microseconds)

        except IOError as e:
            self._logger.debug(e)
            self._logger.debug("Attempt to get created timestamp failed for {0}".format(dir_name))
            self._logger.debug("Assuming that the directory was already processed, returning False")
            return False
    """

    def get_tree_mtime(self, tree_root):
        """
        :param tree_root:
        :rtype: datetime.datetime
        """
        assert os.path.exists(tree_root), "tree_root:{0}, does not exist".format(tree_root)

        FsUtils._logger.debug("tree_root:{0}".format(tree_root))
        latest_mtime = self.try_to_get_dir_last_modified_time(tree_root)
        FsUtils._logger.debug("Setting latest_mtime to {0}".format(latest_mtime))
        for root, dirs, files in os.walk(tree_root, topdown=False):
            for name in dirs:
                mtime = self.try_to_get_dir_last_modified_time(os.path.join(root, name))
                if latest_mtime < mtime:
                    latest_mtime = mtime

        FsUtils._logger.debug("Returning mtime {0}".format(latest_mtime))
        return latest_mtime

    @staticmethod
    def try_to_get_dir_last_modified_time(dir_name):
        try:
            t = os.path.getmtime(dir_name)
            result = datetime.datetime.fromtimestamp(t)
            FsUtils._logger.debug("try_to_get_dir_last_modified_time returning {0}".format(result))
            return result
        except IOError as e:
            FsUtils._logger.debug(e)
            FsUtils._logger.debug(
                "Attempt to get last modified timestamp in try_to_get_dir_last_modified_time failed for {0}".format(
                    dir_name))
            return False

    @staticmethod
    def reacl_tree(target_dir, template_dir, heart_beat_manager=None):
        """
        :param target_dir:
        :param template_dir:
        :type heart_beat_manager: HeartBeatManager
        :return:
        """
        FsUtils._logger.debug("Template:{0}".format(template_dir))
        FsUtils._logger.debug("Target:{0}".format(target_dir))

        get_acl_from_template_command = GetAclCommand(template_dir)
        acl_to_apply = get_acl_from_template_command.execute()
        FsUtils._logger.debug("acl_to_apply:\n{0}".format(acl_to_apply))

        set_acl_on_root_command = SetAclCommand(target_dir, acl_to_apply)
        set_acl_on_root_command.execute()

        for root, dirs, files in os.walk(target_dir, topdown=False):
            for name in files:
                set_acl_on_file_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_file_command.execute()
                if heart_beat_manager:
                    heart_beat_manager.write_heart_beat()
            for name in dirs:
                set_acl_on_dir_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_dir_command.execute()
                if heart_beat_manager:
                    heart_beat_manager.write_heart_beat()

    @staticmethod
    def clear_dir(dir_to_clear):
        for root, dirs, files in os.walk(dir_to_clear, topdown=False):
            for directory in dirs:
                shutil.rmtree(os.path.join(root, directory))