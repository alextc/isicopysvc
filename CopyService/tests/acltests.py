__author__ = 'alextc'
import unittest
import os
from isiapi.getaclcommand import GetAclCommand

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

if __name__ == '__main__':
    unittest.main()

