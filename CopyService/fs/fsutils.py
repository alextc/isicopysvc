__author__ = 'alextc'
import glob
import os
import logging
from isiapi.getaclcommand import GetAclCommand
from isiapi.setaclcommand import SetAclCommand
from aop.logstartandexit import LogEntryAndExit

class FsUtils(object):

    def __init__(self):
        pass

    @staticmethod
    def get_source_directories(root_path):
        logging.debug("\n\tENTERING get_source_directories")
        logging.debug("\n\tPARAMETER root_path\n\t\t%s", root_path)
        result = glob.glob(root_path)
        logging.debug("\n\tRETURNING:\n\t\t%s", "\n\t\t".join(result))
        return result

    @staticmethod
    @LogEntryAndExit(logging.getLogger())
    def reacl_tree(target_dir, template_dir):
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
            for name in dirs:
                set_acl_on_dir_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_dir_command.execute()