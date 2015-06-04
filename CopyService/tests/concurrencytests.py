__author__ = 'alextc'
import unittest
import random
import os


class ConcurrencyTests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_generate_large_number_of_directories(self):
        #setup
        for i in range(5000):
            dir_name = random.randint(10000, 900000)
            dir_path = os.path.join(self._root_path, str(dir_name))
            os.mkdir(dir_path)

if __name__ == '__main__':
    unittest.main()
