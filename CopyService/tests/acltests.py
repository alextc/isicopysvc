__author__ = 'alextc'
import unittest
import os
import random
import logging
import signal
from isiapi.getaclcommand import GetAclCommand
from isiapi.setaclcommand import SetAclCommand
from fs.fsutils import FsUtils


def receive_signal(signum, stack):
    print 'Received:', signum

signal.signal(signal.SIGTERM, receive_signal)


class AclTests(unittest.TestCase):

    '''
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
    '''

    def test_must_reacl_dir_tree(self):
        
        log_message_format = "[%(asctime)s %(process)s: %(message)s"
        logging.basicConfig(filename='/ifs/copy_svc/wip.log',level=logging.DEBUG, format=log_message_format)

        root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"
        template_path = "/ifs/zones/ad1"
        # we're making 5 paths under root_path
        for i in range(5):
            dir_name = random.randint(10000, 900000)
            dir_path = os.path.join(root_path, str(dir_name))
            os.mkdir(dir_path)
            # making 10 files under each new directories created
            for j in range(10):
                file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
                f = open(file_name,'w+')
                f.close()

        get_template_acl_command = GetAclCommand(template_path)
        get_template_acl_command.execute()
        FsUtils.reacl_tree(root_path, template_path)

        print "SetAclCommand Executed {0} times".format(SetAclCommand._function_call_count)
        logging.debug("SetAclCommand Executed {0} times".format(SetAclCommand._function_call_count))

if __name__ == '__main__':
    unittest.main()

