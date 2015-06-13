__author__ = 'alextc'
from fs.fsutils import FsUtils
from isiapi.getsmblockscommand import GetSmbLocksCommand
from sql.phase1db import Phase1Db
from common.datetimeutils import DateTimeUtils
import sys
import logging


class Phase1(object):
    def __init__(self, root_dir, db_path):
        format_logging = "[%(asctime)s %(process)s %(message)s"
        logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=format_logging)
        self._root_dir = root_dir
        self._db_path = db_path

    def get_sources(self):
        fs = FsUtils()
        result = fs.get_source_directories(self._root_dir)
        return result

    @staticmethod
    def get_smb_locks(trim_to):
        get_smb_locks_command = GetSmbLocksCommand(trim_to)
        result = get_smb_locks_command.execute()
        return result

    def clear_db(self):
        db_wrapper = Phase1Db(self._db_path)
        db_wrapper.clear_work_items()

    def write_locks_to_db(self, sources, locks):
        datetime_wrapper = DateTimeUtils()
        db_wrapper = Phase1Db(self._db_path)
        current_datetime_in_utc = datetime_wrapper.get_current_utc_datetime_as_formatted_string()

        for source in sources:
            if source in locks:
                db_wrapper.insert_or_replace_work_item(source, current_datetime_in_utc)
            else:
                db_wrapper.insert_or_replace_work_item_ignore_if_exists(source, current_datetime_in_utc)

    def dump_db(self):
        print "Dumping Db"
        db_wrapper = Phase1Db(self._db_path)
        db_wrapper.dump()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    phase1 = Phase1("/ifs/zones/*/to/*/*/", "/ifs/copy_svc/openfiles.db")
    source_dirs = phase1.get_sources()
    if not source_dirs:
        logging.info("Nothing to do")
        phase1.clear_db()
        sys.exit()

    write_locks = phase1.get_smb_locks(source_dirs)
    phase1.write_locks_to_db(source_dirs, write_locks)
    phase1.dump_db()