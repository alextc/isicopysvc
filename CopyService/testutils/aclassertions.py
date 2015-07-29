__author__ = 'alextc'
from isiapi.getaclcommand import GetAclCommand
import os


class AclAssertions(object):
    def __init__(self):
        pass

    @staticmethod
    def assert_sid_has_access_to_tree(tree, sids):
        get_acl_on_root_command = GetAclCommand(tree)
        ace = get_acl_on_root_command.execute()
        for sid in sids:
            assert sid in ace, "sid {0} is not on dir {1}. ACE was {2}".format(sid, tree, ace)

        for root, dirs, files in os.walk(tree, topdown=False):
            for name in files:
                get_acl_on_file_command = GetAclCommand(os.path.join(root, name))
                ace = get_acl_on_file_command.execute()
                for sid in sids:
                    assert sid in ace, "sid {0} is not on file {1}. ACE was {2}".format(sid, name, ace)
            for name in dirs:
                get_acl_on_dir_command = GetAclCommand(os.path.join(root, name))
                ace = get_acl_on_dir_command.execute()
                for sid in sids:
                    assert sid in ace, "sid {0} is not on dir {1}. ACE was {2}".format(sid, name, ace)
