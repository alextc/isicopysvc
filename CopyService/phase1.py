__author__ = 'alextc'
from CopyService.fs.fsutils import FsUtils
from CopyService.isiapi.getsmblockscommand import GetSmbLocksCommand
from CopyService.sql.writelockdb import WriteLockDb
from CopyService.Common.datetimewrapper import DateTimeWrapper
import sys

if __name__ == '__main__':
    fs = FsUtils()
    sources = fs.get_source_directories("/ifs/zones/*/to/*/*/")
    if not sources:
        print "Nothing to do"
        sys.exit()

    print "Sources"
    for source in sources:
        print source

    get_smb_locks_command = GetSmbLocksCommand("/ifs/zones/")
    locks = get_smb_locks_command.execute()
    print "Locks"
    for lock in locks:
        print lock

    db_wrapper = WriteLockDb("/ifs/copy_svc/openfiles.db")
    #db_wrapper.clear_last_seen_table()
    datetime_wrapper = DateTimeWrapper()

    for source in sources:
        if source in locks:
            db_wrapper.insert_or_replace_last_seen(
                source,
                datetime_wrapper.get_current_utc_datetime_as_formatted_string())

    print "Dumping Db"
    db_wrapper.dump()


