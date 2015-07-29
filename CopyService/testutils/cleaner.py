__author__ = 'alextc'
from sql.phase1db import Phase1Db
from sql.phase2db import Phase2Db
from fs.fsutils import FsUtils


class Cleaner(object):

    _phase1_source_root_path = "/ifs/zones/ad1/copy_svc/to/ad2"
    _phase2_source_root_path = "/ifs/zones/ad1/copy_svc/staging/ad2"
    _phase2_destination_root_path = "/ifs/zones/ad2/copy_svc/from/ad1"

    @staticmethod
    def clean_phase1():
        # Starting clean
        print "Cleaning Phase1Db"
        Phase1Db().clear_work_items()
        print "Cleaning Phase1 Source Dirs"
        FsUtils.clear_dir(Cleaner._phase1_source_root_path)
        print "Cleaning Phase2 Source Dirs"
        FsUtils.clear_dir(Cleaner._phase2_source_root_path)

    @staticmethod
    def clean_phase2():
        # Starting clean
        print "Cleaning Phase2Db"
        Phase2Db().clear_heart_beat_table()
        print "Cleaning Phase2 Source Dirs"
        FsUtils.clear_dir(Cleaner._phase2_source_root_path)
        print "Cleaning Phase2 Destination Dirs"
        FsUtils.clear_dir(Cleaner._phase2_destination_root_path)