import unittest
import datetime
from lnx.stat import Stat
from testutils.workitemsfactory import WorkItemsFactory

class DirBirthDateTests(unittest.TestCase):

    def test_must_get_birth_date_of_dir(self):
        sut = WorkItemsFactory().create_phase1_work_item()
        birth_datetime = Stat.get_birth_time(sut.phase1_source_dir)
        self.assertTrue(type(birth_datetime) is datetime.datetime)
        assert (datetime.datetime.now() - birth_datetime).seconds < 1

if __name__ == '__main__':
    unittest.main()