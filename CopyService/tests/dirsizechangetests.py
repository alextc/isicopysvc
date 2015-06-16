__author__ = 'alextc'
import unittest
import random
import os
import logging
from lnx.du import DiskUsage


class DirSizeChangeTests(unittest.TestCase):

    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_dir_size_must_increase_when_new_file_is_added(self):
        dir_name = random.randint(10000, 900000)
        dir_path = os.path.join(DirSizeChangeTests._root_path, str(dir_name))
        os.mkdir(dir_path)
        orig_size = os.path.getsize(dir_path)
        for j in range(3):
            file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
            f = open(file_name, 'w+')
            f.write("Hello")
            f.close()
            self.assertTrue(os.path.getsize(dir_path) > orig_size)

    def test_dir_size_does_not_depend_on_sizes_of_files_it_contains(self):
        dir_name = random.randint(10000, 900000)
        dir_path = os.path.join(DirSizeChangeTests._root_path, str(dir_name))
        os.mkdir(dir_path)

        file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
        f = open(file_name, 'w+')
        orig_size = os.path.getsize(dir_path)
        for i in range(1000):
            f.write("Hello00000000000000000000000000000000000000000000000000000000000000000000000000")
        f.close()
        self.assertTrue(os.path.getsize(dir_path) == orig_size)

    def test_dir_size_must_not_increase_when_new_file_is_added_in_sub_dir(self):
        dir_name = random.randint(10000, 900000)
        sub_dir_name = random.randint(10000, 900000)
        parent_dir_path = os.path.join(DirSizeChangeTests._root_path, str(dir_name))
        os.mkdir(parent_dir_path)
        sub_dir_path = os.path.join(str(parent_dir_path), str(sub_dir_name))
        os.mkdir(sub_dir_path)
        orig_size = os.path.getsize(parent_dir_path)
        for j in range(3):
            file_name = os.path.join(sub_dir_path, str(random.randint(10000, 900000)))
            f = open(file_name, 'w+')
            f.write("Hello")
            f.close()
            self.assertTrue(os.path.getsize(parent_dir_path) == orig_size)

    def test_du_must_return_dir_size(self):
        dir_name = random.randint(10000, 900000)
        dir_path = os.path.join(DirSizeChangeTests._root_path, str(dir_name))
        os.mkdir(dir_path)
        file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
        f = open(file_name, 'w+')
        for i in range(1000):
            f.write("Hello00000000000000000000000000000000000000000000000000000000000000000000000000")
        f.close()

        disk_usage = DiskUsage.get_disk_usage(dir_path)
        print disk_usage

if __name__ == '__main__':
    format_logging = "[%(asctime)s %(process)s %(message)s"
    logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=format_logging)
    unittest.main()