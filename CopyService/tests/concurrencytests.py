__author__ = 'alextc'
import unittest
import random
import os


class ConcurrencyTests(unittest.TestCase):
    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_generate_large_number_of_directories(self):
        #setup
        for i in range(50):
            dir_name = random.randint(10000, 900000)
            dir_path = os.path.join(ConcurrencyTests._root_path, str(dir_name))
            os.mkdir(dir_path)
            for j in range(10):
                file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
                f = open(file_name,'w+')
                f.close()

if __name__ == '__main__':
    unittest.main()
