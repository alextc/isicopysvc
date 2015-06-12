__author__ = 'alextc'
import sqlite3
import os
from common.datetimeutils import DateTimeUtils
from model.phase1workitem import Phase1WorkItem


class WriteLockDb:

    def __init__(self, data_file_path):
        if not os.path.exists(os.path.dirname(data_file_path)):
            raise ValueError("Directory does not exist '{}'".format(os.path.dirname(data_file_path)))

        self._data_file_path = data_file_path
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='openfiles'")
            result = cursor.fetchone();
            if not result:
                cursor.execute("CREATE TABLE openfiles "
                               "(directory TEXT NOT NULL, "
                               "directory_last_modified timestamp NOT NULL,"
                               "last_smb_write_lock timestamp NOT NULL, "
                               "PRIMARY KEY (directory, directory_last_modified))")

    def insert_or_replace_last_seen(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT or REPLACE into openfiles ' \
                           '(directory,directory_last_modified, last_smb_write_lock ) VALUES (?,?,?)'
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.phase1_source_dir,
                phase1_work_item.last_modified,
                phase1_work_item.last_smb_lock_detected))

    def insert_or_replace_last_seen_ignore_if_exists(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """
        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT or IGNORE into openfiles ' \
                           '(directory,directory_last_modified, last_smb_write_lock ) VALUES (?,?,?)'
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.phase1_source_dir,
                phase1_work_item.last_modified,
                phase1_work_item.last_smb_lock_detected))

    def get_last_seen_record_for_dir(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :rtype: Phase1WorkItem
        """

        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM openfiles WHERE directory=? AND last_modified=?',
                           (phase1_work_item.phase1_source_dir),
                           (phase1_work_item.last_modified))
            result = cursor.fetchone()

        if result:
            phase1_work_item = Phase1WorkItem(phase1_source_dir=result[0], last_modified=result[1])
            phase1_work_item.last_smb_lock_detected = result[2]
            return phase1_work_item

    def delete_last_seen_records(self, path):
        with sqlite3.connect(self._data_file_path) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from openfiles WHERE directory = ?"
            cursor.execute(sql_delete_query, [path])


    def clear_last_seen_table(self):
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from openfiles"
            cursor.execute(sql_delete_query)
