__author__ = 'alextc'
import glob
import os
import logging
import datetime
from isiapi.getaclcommand import GetAclCommand
from isiapi.setaclcommand import SetAclCommand
from aop.logstartandexit import LogEntryAndExit
from cluster.heartbeatmanager import HeartBeatManager


class FsUtils(object):

    def __init__(self):
        pass

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def get_source_directories(root_path):
        logging.debug("\n\tPARAMETER root_path\n\t\t%s", root_path)
        result = glob.glob(root_path)
        logging.debug("\n\tRETURNING:\n\t\t%s", "\n\t\t".join(result))
        return result

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def try_to_get_dir_last_modified_time(dir_name):
        """
        :type dir_name: str
        :rtype: datetime.datetime
        """
        try:
            t = os.path.getmtime(dir_name)
            result = datetime.datetime.fromtimestamp(t)
            logging.debug("Returning {0}".format(result))
            return result
        except IOError as e:
            logging.debug(e)
            logging.debug("Attempt to get last modified timestamp failed for {0}".format(dir_name))
            logging.debug("Assuming that the directory was already processed, returning False")
            return False

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def try_to_get_dir_created_time(dir_name):
        """
        :type dir_name: str
        :rtype: datetime.datetime
        """
        try:
            t = os.path.getctime(dir_name)
            return datetime.datetime.fromtimestamp(t)
        except IOError as e:
            logging.debug(e)
            logging.debug("Attempt to get created timestamp failed for {0}".format(dir_name))
            logging.debug("Assuming that the directory was already processed, returning False")
            return False

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def get_tree_mtime(tree_root):
        """
        :param tree_root:
        :rtype: datetime.datetime
        """
        assert os.path.exists(tree_root), "tree_root:{0}, does not exist".format(tree_root)

        logging.debug("tree_root:{0}".format(tree_root))
        latest_mtime = FsUtils.try_to_get_dir_last_modified_time(tree_root)
        logging.debug("Setting latest_mtime to {0}".format(latest_mtime))
        for root, dirs, files in os.walk(tree_root, topdown=False):
            for name in dirs:
                mtime = FsUtils.try_to_get_dir_last_modified_time(os.path.join(root, name))
                if latest_mtime < mtime:
                    latest_mtime = mtime

        logging.debug("Returning mtime {0}".format(latest_mtime))
        return latest_mtime

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def reacl_tree(target_dir, template_dir, heart_beat_manager):
        """
        :param target_dir:
        :param template_dir:
        :type heart_beat_manager: HeartBeatManager
        :return:
        """
        logging.debug("Template:{0}".format(template_dir))
        logging.debug("Target:{0}".format(target_dir))

        get_acl_from_template_command = GetAclCommand(template_dir)
        acl_to_apply = get_acl_from_template_command.execute()

        # I don't think I need to set ACL here /ifs/zones/ad1/copy_svc/staging/ad2 ???
        set_acl_on_root_command = SetAclCommand(target_dir, acl_to_apply)
        set_acl_on_root_command.execute()

        for root, dirs, files in os.walk(target_dir, topdown=False):
            for name in files:
                set_acl_on_file_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_file_command.execute()
                heart_beat_manager.write_heart_beat()
            for name in dirs:
                set_acl_on_dir_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_dir_command.execute()
                heart_beat_manager.write_heart_beat()