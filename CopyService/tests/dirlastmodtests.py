__author__ = 'alextc'
import unittest
import time
import random
import os
from fs.fsutils import FsUtils


class DirLastModifiedTests(unittest.TestCase):

    _root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"

    def test_mtime_on_dir_must_change_when_file_in_this_dir_is_modified(self):
        dir_name = random.randint(10000, 900000)
        dir_path = os.path.join(DirLastModifiedTests._root_path, str(dir_name))
        os.mkdir(dir_path)
        mtime = FsUtils.try_to_get_dir_last_modified_time(dir_path)
        for j in range(3):
            file_name = os.path.join(dir_path, str(random.randint(10000, 900000)))
            f = open(file_name, 'w+')
            f.close()
            time.sleep(1)
            self.assertTrue(mtime < FsUtils.try_to_get_dir_last_modified_time(dir_path))
            mtime = FsUtils.try_to_get_dir_last_modified_time(dir_path)

    def test_mtime_on_dir_must_not_change_when_file_in_the_sub_dir_is_modified(self):
        dir_name = random.randint(10000, 900000)
        sub_dir_name = random.randint(10000, 900000)
        parent_dir_path = os.path.join(DirLastModifiedTests._root_path, str(dir_name))
        os.mkdir(parent_dir_path)
        sub_dir_path = os.path.join(str(parent_dir_path), str(sub_dir_name))
        os.mkdir(sub_dir_path)
        mtime_parent_dir = FsUtils.try_to_get_dir_last_modified_time(parent_dir_path)
        for j in range(3):
            file_name = os.path.join(sub_dir_path, str(random.randint(10000, 900000)))
            f = open(file_name, 'w+')
            f.close()
            time.sleep(1)
            mtime_sub_dir = FsUtils.try_to_get_dir_last_modified_time(sub_dir_path)
            self.assertTrue(mtime_parent_dir < mtime_sub_dir)

if __name__ == '__main__':
    unittest.main()
