__author__ = 'alextc'
import unittest
import os
import logging
from isiapi.setaclcommand import SetAclCommand


class AclFactoryTests(unittest.TestCase):

    def test_must_get_acl_for_directory_from_template(self):

        # AD2 ISI_ADMINS = S-1-5-21-2576225250-2004976870-3728844968-1108
        # AD2 ISI_READERS = S-1-5-21-2576225250-2004976870-3728844968-1106
        # AD2 ISI_WRITERS = S-1-5-21-2576225250-2004976870-3728844968-1107
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
        dir_path = "/ifs/zones/ad2/copy_svc/from/"

        self.assertTrue(os.path.exists(dir_path))
        SetAclCommand(dir_path, acl_template).execute()

if __name__ == '__main__':
    log_message_format = "[%(asctime)s %(process)s: %(message)s"
    logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=log_message_format)
    unittest.main()