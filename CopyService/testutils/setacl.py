__author__ = 'alextc'
import os
from isiapi.setaclcommand import SetAclCommand


class SetAcl(object):

    # AD2 ISI_ADMINS = S-1-5-21-2576225250-2004976870-3728844968-1108
    # AD2 ISI_READERS = S-1-5-21-2576225250-2004976870-3728844968-1106
    # AD2 ISI_WRITERS = S-1-5-21-2576225250-2004976870-3728844968-1107

    # Removing "authoritative" : "acl" breaks the API - Investigate further

    acl_template = """{
    "acl" :
    [
        {
            "accessrights" : [ "dir_gen_read", "dir_gen_execute" ],
            "accesstype" : "allow",
            "inherit_flags" : [ "object_inherit", "container_inherit" ],
            "trustee" :
            {
                "id" : "SID:S-1-5-21-2576225250-2004976870-3728844968-1106"
            }
        },

        {
            "accessrights" : [ "dir_gen_all" ],
            "accesstype" : "allow",
            "inherit_flags" : [ "object_inherit", "container_inherit" ],
            "trustee" :
            {
                "id" : "SID:S-1-5-21-2576225250-2004976870-3728844968-1108"
            }
        },

        {
            "accessrights" : [ "dir_gen_read", "dir_gen_write", "dir_gen_execute" ],
            "accesstype" : "allow",
            "inherit_flags" : [ "object_inherit", "container_inherit" ],
            "trustee" :
            {
                "id" : "SID:S-1-5-21-2576225250-2004976870-3728844968-1107"
            }
        }
        ],
    "authoritative" : "acl",
        "group" :
        {
            "id" : "GID:0",
            "name" : "wheel",
            "type" : "group"
        },
        "mode" : "0775",
        "owner" :
        {
            "id" : "UID:0",
            "name" : "root",
            "type" : "user"
        }
    }"""

    # dir_path = "/ifs/zones/ad2/copy_svc/from/ad1"
    dir_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    @staticmethod
    def reacl_tree(target_dir, acl_to_apply):
        """
        :param target_dir:
        :param acl_to_apply:
        :return:
        """

        set_acl_on_root_command = SetAclCommand(target_dir, acl_to_apply)
        set_acl_on_root_command.execute()

        for root, dirs, files in os.walk(target_dir, topdown=False):
            for name in files:
                set_acl_on_file_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_file_command.execute()
            for name in dirs:
                set_acl_on_dir_command = SetAclCommand(os.path.join(root, name), acl_to_apply)
                set_acl_on_dir_command.execute()

if __name__ == '__main__':
    SetAcl.reacl_tree(SetAcl.dir_path, SetAcl.acl_template)