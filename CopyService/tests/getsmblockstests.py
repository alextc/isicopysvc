__author__ = 'alextc'
import unittest
from CopyService.isiapi.getsmblockscommand import GetSmbLocksCommand

class GetSmbLocksTests(unittest.TestCase):

    def test_must_return_a_record_per_locked_file(self):
        paths_to_test = ['/ifs/zones/ad1/to/ad2/']
        papi_command = GetSmbLocksCommand(paths_to_test)
        result = papi_command.execute()

        # Assumes that files locked for write exist on tested path
        # Expect lockfile1.txt and lockfile2.txt
        self.assertTrue(len(result) == 1)
        for path in result:
            self.assertTrue(path.startswith(paths_to_test[0]))

if __name__ == '__main__':
    unittest.main()