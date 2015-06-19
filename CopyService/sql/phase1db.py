__author__ = 'alextc'
import sqlite3
import os
import datetime
import logging
from datetime import timedelta
from model.phase1workitem import Phase1WorkItem
from common.datetimeutils import DateTimeUtils
from aop.logstartandexit import LogEntryAndExit


class Phase1Db:
    _data_file_path = "/ifs/copy_svc/phase1.db"

    def __init__(self):
        if not os.path.exists(os.path.dirname(Phase1Db._data_file_path)):
            raise ValueError("Directory does not exist '{}'".format(os.path.dirname(Phase1Db._data_file_path)))

        self._data_file_path = Phase1Db._data_file_path
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

    def add_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT INTO phase1_work_items ' \
                           '(directory, created, last_modified, last_smb_write_lock ) VALUES (?,?,?,?)'
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.phase1_source_dir,
                phase1_work_item.tree_creation_time,
                phase1_work_item.tree_last_modified,
                phase1_work_item.last_smb_write_lock))

    def update_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'UPDATE phase1_work_items ' \
                           'SET last_modified = ?, last_smb_write_lock= ? WHERE directory= ? AND created = ?'
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.tree_last_modified,
                phase1_work_item.last_smb_write_lock,
                phase1_work_item.phase1_source_dir,
                phase1_work_item.tree_creation_time,))

    @LogEntryAndExit(logging.getLogger())
    def get_still_work_items(self, stillness_threshold_in_sec):
        result = []
        # sqlite needs date in string format when comparing
        # http://stackoverflow.com/questions/1975737/sqlite-datetime-comparison
        # threshold = DateTimeUtils().datetime_to_formatted_string(
        #    datetime.datetime.now() - timedelta(seconds=stillness_threshold_in_sec))

        threshold = datetime.datetime.now() - timedelta(seconds=stillness_threshold_in_sec)
        logging.debug("Calculated threshold {0}".format(threshold))

        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            params = (threshold, )
            cursor.execute('SELECT * FROM phase1_work_items WHERE last_smb_write_lock < ?', params)
            qry_results = cursor.fetchall()

            if not qry_results:
                return result

            for qry_result in qry_results:
                result.append(
                    Phase1WorkItem(
                        source_dir=qry_result["directory"],
                        tree_creation_time=qry_result["created"],
                        tree_last_modified=qry_result["last_modified"],
                        smb_write_lock_last_seen=qry_result["last_smb_write_lock"]))

            return result

    def get_work_item(self, source_dir, ctime):
        """
        :type source_dir: str
        :type ctime: datetime.datetime
        :rtype: Phase1WorkItem
        """

        assert \
            os.path.exists(source_dir),\
            "Phase1 Db was requested to search for a dir:{0} that does not exist on fs".format(source_dir)

        # sqlite needs date in string format when comparing
        # http://stackoverflow.com/questions/1975737/sqlite-datetime-comparison
        # created_param = DateTimeUtils().datetime_to_formatted_string(ctime)
        logging.debug("About to perform search for {0} : {1}".format(source_dir, ctime))
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            params = (source_dir, ctime)
            cursor.execute('SELECT * FROM phase1_work_items WHERE directory=? AND created=?', params)
            result = cursor.fetchall()

            assert (len(result) == 0 or len(result) == 1), \
                "Phase1 Db is corrupted, only one record should exist per dir_name, ctime combo"

            if not result:
                return None

            phase1_work_item = Phase1WorkItem(
                source_dir=result[0]["directory"],
                tree_creation_time=result[0]["created"],
                tree_last_modified=result[0]["last_modified"],
                smb_write_lock_last_seen=result[0]["last_smb_write_lock"])

            return phase1_work_item

    def remove_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        """
        with sqlite3.connect(self._data_file_path) as connection:
            cursor = connection.cursor()
            sql_delete_query = "DELETE FROM phase1_work_items WHERE directory = ? AND created=?"
            params = (phase1_work_item.phase1_source_dir, phase1_work_item.tree_creation_time)
            cursor.execute(sql_delete_query, params)

    def clear_work_items(self):
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            sql_delete_query = "DELETE FROM phase1_work_items"
            cursor.execute(sql_delete_query)
