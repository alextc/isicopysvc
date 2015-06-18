import unittest
import os
import random
from sql.phase1db import Phase1Db
from fs.fsutils import FsUtils
from model.phase1workitem import Phase1WorkItem


class Phase1DbTests(unittest.TestCase):
    _db_path = "/ifs/copy_svc/phase1.db"
    _root_path = "/ifs/zones/ad1/copy_svc/to/ad2"

    def test_init_must_create_new_db_file(self):
        if os.path.exists(self._db_path):
            os.remove(self._db_path)
        Phase1Db(self._db_path)
        self.assertTrue(os.path.exists(self._db_path))

    def test_must_insert_new_lock_file_record(self):
        phase1_work_item = self.create_phase1_work_item()
        phase1_db = Phase1Db(Phase1DbTests._db_path)
        phase1_db.add_or_update_work_item(phase1_work_item)
        confirm = phase1_db.get_work_item(phase1_work_item)
        self.assertEquals(phase1_work_item, confirm)

    def create_phase1_work_item(self):
        phase1_source_dir_name = random.randint(10000, 900000)
        phase1_source_dir_path = os.path.join(Phase1DbTests._root_path, str(phase1_source_dir_name))
        os.mkdir(phase1_source_dir_path)
        created = FsUtils.try_to_get_dir_created_time(phase1_source_dir_path)
        last_modified = FsUtils.try_to_get_dir_last_modified_time(phase1_source_dir_path)
        phase1_work_item = Phase1WorkItem(
            source_dir=phase1_source_dir_path,
            tree_creation_time=last_modified,
            tree_last_modified=last_modified)
        return phase1_work_item


if __name__ == '__main__':
    unittest.main()
