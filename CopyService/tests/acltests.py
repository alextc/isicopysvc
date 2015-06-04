__author__ = 'alextc'
import unittest
import os
from isiapi.getaclcommand import GetAclCommand
from isiapi.setaclcommand import SetAclCommand

class AclTests(unittest.TestCase):

    def test_must_get_acl_for_directory(self):
        dir_path = "/ifs/zones/ad1/to/ad2"
        self.assertTrue(os.path.exists(dir_path))
        print "About to get ACL from " + dir_path
        get_acl_command = GetAclCommand(dir_path)
        acl = get_acl_command.execute()
        print "Command Parameters"
        print get_acl_command
        print acl
        self.assertTrue(acl)

    def test_must_get_and_then_setacl_for_directory(self):
        template_dir = "/ifs/zones/ad1/to/ad2"
        destination_dir = "/ifs/zones/ad2/from/ad1"
        self.assertTrue(os.path.exists(template_dir))
        self.assertTrue(os.path.exists(destination_dir))

        get_acl_command = GetAclCommand(template_dir)
        acl = get_acl_command.execute()
        self.assertTrue(acl)
        # this will throw an exception if it does not work
        SetAclCommand(destination_dir, acl)

if __name__ == '__main__':
    unittest.main()

