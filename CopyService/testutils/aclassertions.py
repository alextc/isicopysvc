__author__ = 'alextc'
import os
from isiapi.getaclcommand import GetAclCommand
from isiapi.acl import Acl
from isiapi.ace import Ace
from log.loggerfactory import LoggerFactory


class AclAssertions(object):
    _logger = LoggerFactory().create("AclAssertions")

    def __init__(self):

        # TODO: Instead of using SIDS, create a function that would convert AD name to SID
        # Ex. isi auth groups view --zone AD1 --sid S-1-5-21-1415853412-1264766915-3061632431-1107
        # this converts sid to name
        # So if config I define zone and AD group, to resolve to SID
        # isi auth groups view --zone AD1 --group 'AD1\isi_readers'

        # TODO: Initialize this from config
        expected_dir_admin_ace = \
            Ace("SID:S-1-5-21-2576225250-2004976870-3728844968-1108", ["dir_gen_all"])
        expected_dir_rw_ace = \
            Ace("SID:S-1-5-21-2576225250-2004976870-3728844968-1107",
                ["dir_gen_read", "dir_gen_write", "dir_gen_execute"])
        expected_dir_ro_ace = \
            Ace("SID:S-1-5-21-2576225250-2004976870-3728844968-1106", ["dir_gen_read", "dir_gen_execute"])
        self._expected_dir_acl = Acl(aces=[expected_dir_ro_ace, expected_dir_admin_ace, expected_dir_rw_ace])

        expected_file_admin_ace = \
            Ace("SID:S-1-5-21-2576225250-2004976870-3728844968-1108", ["file_gen_all"])
        expected_file_rw_ace = \
            Ace("SID:S-1-5-21-2576225250-2004976870-3728844968-1107",
                ["file_gen_read", "file_gen_write", "file_gen_execute"])
        expected_file_ro_ace = \
            Ace("SID:S-1-5-21-2576225250-2004976870-3728844968-1106", ["file_gen_read", "file_gen_execute"])
        self._expected_file_acl = Acl(aces=[expected_file_ro_ace, expected_file_admin_ace, expected_file_rw_ace])

    def assert_acl_applied(self, tree):
        self._test_ace(tree, self._expected_dir_acl)
        for root, dirs, files in os.walk(tree, topdown=False):
            for name in files:
                self._test_ace(os.path.join(root, name), self._expected_file_acl)
            for name in dirs:
                self._test_ace(os.path.join(root, name), self._expected_dir_acl)

    @staticmethod
    def _test_ace(fs_object, expected_acl):
        get_acl_command = GetAclCommand(fs_object)
        raw_acl = get_acl_command.execute()
        assert raw_acl, "Failed to get raw_acl from ISI API"
        print raw_acl
        sut = Acl(get_acl_raw_output=raw_acl)

        AclAssertions._logger.debug("About to compare ACLs")
        AclAssertions._logger.debug("Applied ACL:\n{0}\nExpected ACL:\n{1}".format(sut, expected_acl))
        assert sut == expected_acl, \
                    "Applied ACL:\n{0}\n did not match the expected ACL:\n{1}".format(sut, expected_acl)
