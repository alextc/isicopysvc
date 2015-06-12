__author__ = 'alextc'
import unittest
import random
import os
from model.phase1workitem import Phase1WorkItem
from fs.fsutils import FsUtils


class Phase1ItemTests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/to/ad2"

    def test_generate_large_number_of_directories(self):

        phase1_source_dir_name = random.randint(10000, 900000)
        phase1_source_dir_path = os.path.join(Phase1ItemTests._root_path, str(phase1_source_dir_name))
        os.mkdir(phase1_source_dir_path)

        last_modified = FsUtils.try_to_get_dir_last_modified_time(phase1_source_dir_path)
        sut = Phase1WorkItem(phase1_source_dir=phase1_source_dir_path, last_modified=last_modified)
        print sut

if __name__ == '__main__':
    unittest.main()
