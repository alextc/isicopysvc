__author__ = 'alextc'
from CopyService.fs.fsutils import FsUtils
from CopyService.isiapi.getsmblockscommand import GetSmbLocksCommand
from CopyService.sql.writelockdb import WriteLockDb
from CopyService.Common.datetimewrapper import DateTimeWrapper
import sys
import logging

class Phase1(object):
    def __init__(self, root_dir, db_path):
        self._root_dir = root_dir
        self._db_path = db_path

    def get_sources(self):
        fs = FsUtils()
        result = fs.get_source_directories(self._root_dir)
        return result

    def get_smb_locks(self, trim_to):
        get_smb_locks_command = GetSmbLocksCommand(trim_to)
        result = get_smb_locks_command.execute()
        return result

    def clear_db(self):
        db_wrapper = WriteLockDb(self._db_path)
        db_wrapper.clear_last_seen_table()

    def write_locks_to_db(self, sources, locks):
        datetime_wrapper = DateTimeWrapper()
        db_wrapper = WriteLockDb(self._db_path)

        for source in sources:
            if source in locks:
                db_wrapper.insert_or_replace_last_seen(
                    source,
                    datetime_wrapper.get_current_utc_datetime_as_formatted_string())

    def dump_db(self):
        print "Dumping Db"
        db_wrapper = WriteLockDb(self._db_path)
        db_wrapper.dump()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    phase1 = Phase1("/ifs/zones/*/to/*/*/", "/ifs/copy_svc/openfiles.db")
    # TODO: remove this line - testing
    phase1.clear_db()
    source_dirs = phase1.get_sources()
    if not source_dirs:
        print "Nothing to do"
        phase1.clear_db()
        sys.exit()

    write_locks = phase1.get_smb_locks(source_dirs)
    phase1.write_locks_to_db(source_dirs, write_locks)
    phase1.dump_db()

