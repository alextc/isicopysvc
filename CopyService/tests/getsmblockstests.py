__author__ = 'alextc'
import unittest
from CopyService.isiapi.papicommands import GetSmbLocksCommand

class GetSmbLocksTests(unittest.TestCase):

    def test_must_return_a_record_per_locked_file(self):
        papi_command = GetSmbLocksCommand()
        print papi_command
        result = papi_command.execute()
        print result

if __name__ == '__main__':
    unittest.main()