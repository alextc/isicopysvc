__author__ = 'alextc'
import sqlite3
import os
from model.phase1workitem import Phase1WorkItem


class Phase1Db:

    def __init__(self, data_file_path):
        if not os.path.exists(os.path.dirname(data_file_path)):
            raise ValueError("Directory does not exist '{}'".format(os.path.dirname(data_file_path)))

        self._data_file_path = data_file_path
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phase1_work_items'")
            result = cursor.fetchone()
            if not result:
                cursor.execute("CREATE TABLE phase1_work_items "
                               "(directory TEXT NOT NULL, "
                               "created timestamp NOT NULL, "
                               "last_modified timestamp NOT NULL,"
                               "last_smb_write_lock timestamp NOT NULL, "
                               "PRIMARY KEY (directory, created))")

    def insert_or_replace_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT or REPLACE into phase1_work_items ' \
                           '(directory, created, last_modified, last_smb_write_lock ) VALUES (?,?,?,?)'
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.phase1_source_dir,
                phase1_work_item.dir_creation_time,
                phase1_work_item.dir_last_modified,
                phase1_work_item.last_smb_write_lock))

    def insert_or_replace_work_item_ignore_if_exists(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """
        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT or IGNORE into phase1_work_items ' \
                           '(directory, created, last_modified, last_smb_write_lock ) VALUES (?,?,?,?)'
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.phase1_source_dir,
                phase1_work_item.dir_creation_time,
                phase1_work_item.last_smb_write_lock))

    def get_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :rtype: Phase1WorkItem
        """

        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            params = (phase1_work_item.phase1_source_dir, phase1_work_item.dir_creation_time)
            cursor.execute('SELECT * FROM phase1_work_items WHERE directory=? AND created=?', params)
            result = cursor.fetchone()

        if result:
            phase1_work_item = Phase1WorkItem(
                source_dir=result["directory"],
                dir_creation_time=result["created"],
                dir_last_modified=result["last_modified"])
            phase1_work_item.last_smb_write_lock = result["last_smb_write_lock"]
            return phase1_work_item

    def remove_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        """
        with sqlite3.connect(self._data_file_path) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from phase1_work_items WHERE directory = ? AND created=?"
            params = (phase1_work_item.phase1_source_dir, phase1_work_item.dir_creation_time)
            cursor.execute(sql_delete_query, params)

    def clear_work_items(self):
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from phase1_work_items"
            cursor.execute(sql_delete_query)