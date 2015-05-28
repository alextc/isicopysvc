__author__ = 'alextc'
import unittest
from CopyService.isiapi.getsmblockscommand import GetSmbLocksCommand

class GetSmbLocksTests(unittest.TestCase):

    def test_must_return_a_record_per_locked_file(self):
        path_to_test = '/ifs/zones/ad1'
        papi_command = GetSmbLocksCommand(path_to_test)
        result = papi_command.execute()

        # Assumes that files locked for write exist on tested path
        # Expect lockfile1.txt and lockfile2.txt
        self.assertTrue(len(result) == 2)
        for path in result:
            self.assertTrue(path.startswith(path_to_test))


if __name__ == '__main__':
    unittest.main()