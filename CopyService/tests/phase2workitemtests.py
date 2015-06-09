__author__ = 'alextc'
import unittest
import random
import os

from model.phase2workitem import Phase2WorkItem

class Phase2ItemTests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_generate_large_number_of_directories(self):

        phase2_source_dir_name = random.randint(10000, 900000)
        phase2_source_dir_path = os.path.join(Phase2ItemTests._root_path, str(phase2_source_dir_name))
        os.mkdir(phase2_source_dir_path)

        sut = Phase2WorkItem(phase2_source_dir=phase2_source_dir_path, state="Init")
        print sut

if __name__ == '__main__':
    unittest.main()

