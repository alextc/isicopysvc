__author__ = 'alextc'
import unittest
from isiapi.getsmbsharescommand import GetSmbSharesCommand

class GetSmbSharesTests(unittest.TestCase):

    def test_must_return_a_record_per_smb_share(self):
        root_path = '/ifs/zones/'
        papi_command = GetSmbSharesCommand(root_path)
        result = papi_command.execute()

        # I have only one share that matches the root_path
        self.assertTrue(len(result) == 1)
        for share in result:
            self.assertTrue(share.startswith(root_path))

        print "\n".join(result)

if __name__ == '__main__':
    unittest.main()