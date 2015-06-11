import unittest
import os
from sql.writelockdb import WriteLockDb
from common.datetimeutils import DateTimeUtils


class WriteLockDbTests(unittest.TestCase):
    _db_path = "/ifs/copy_svc/files.db"

    def test_init_must_create_new_db_file(self):
        if os.path.exists(self._db_path):
            os.remove(self._db_path)
        WriteLockDb(self._db_path)
        self.assertTrue(os.path.exists(self._db_path))

    def test_must_insert_new_lock_file_record(self):
        wrapper = WriteLockDb(self._db_path)
        datetime_wrapper = DateTimeUtils()
        path = "/ifs/zones/ad1"
        dt = datetime_wrapper.get_current_utc_datetime_as_formatted_string()
        wrapper.insert_or_replace_last_seen(path, dt)

        inserted_record = wrapper.get_last_seen_record_for_dir(path)

        self.assertEquals(path, inserted_record['directory'])
        self.assertEquals(dt, inserted_record['last_write_lock'])

    def test_must_delete_lock_file_record(self):
        wrapper = WriteLockDb(self._db_path)

        # Setup
        datetime_wrapper = DateTimeUtils()
        path = "/ifs/zones/ad1"
        dt = datetime_wrapper.get_current_utc_datetime_as_formatted_string()
        wrapper.insert_or_replace_last_seen(path, dt)
        inserted_record = wrapper.get_last_seen_record_for_dir(path)
        self.assertEquals(path, inserted_record['directory'])
        self.assertEquals(dt, inserted_record['last_write_lock'])

        # Test
        wrapper.delete_last_seen_records(path)
        deleted_record = wrapper.get_last_seen_record_for_dir(path)

        # Validate
        self.assertFalse(deleted_record)

if __name__ == '__main__':
    unittest.main()
