__author__ = 'alextc'
import sqlite3
import os
import datetime
import logging
from datetime import timedelta
from model.phase1workitem import Phase1WorkItem
from log.loggerfactory import LoggerFactory
from sqlitefacade import SqliteFacade


class Phase1Db:
    _data_file_path = "/ifs/copy_svc/phase1.db"
    _logger = LoggerFactory.create('Phase1Db')

    def __init__(self):
        if not os.path.exists(os.path.dirname(Phase1Db._data_file_path)):
            raise ValueError("Directory does not exist '{0}'".format(os.path.dirname(Phase1Db._data_file_path)))

        self._data_file_path = Phase1Db._data_file_path
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phase1_work_items'")
            result = cursor.fetchone()
            if not result:
                cursor.execute("CREATE TABLE phase1_work_items "
                               "(directory TEXT NOT NULL, "
                               "last_modified timestamp NOT NULL,"
                               "last_smb_write_lock timestamp NOT NULL, "
                               "PRIMARY KEY (directory))")

    def add_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT INTO phase1_work_items ' \
                           '(directory, last_modified, last_smb_write_lock ) VALUES (?,?,?)'
        parameters = (phase1_work_item.phase1_source_dir,
                      phase1_work_item.tree_last_modified,
                      phase1_work_item.last_smb_write_lock)

        SqliteFacade.execute_parameterized_command(self._data_file_path, sql_insert_query, parameters)

    def update_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'UPDATE phase1_work_items ' \
                           'SET last_modified = ?, last_smb_write_lock= ? WHERE directory= ?'

        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (
                phase1_work_item.tree_last_modified,
                phase1_work_item.last_smb_write_lock,
                phase1_work_item.phase1_source_dir))

    def get_still_work_items(self, stillness_threshold_in_sec):
        result = []

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
                        tree_last_modified=qry_result["last_modified"],
                        smb_write_lock_last_seen=qry_result["last_smb_write_lock"]))

            return result

    def get_all_work_items(self):
        result = []

        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM phase1_work_items')
            qry_results = cursor.fetchall()

            if not qry_results:
                return result

            for qry_result in qry_results:
                result.append(
                    Phase1WorkItem(
                        source_dir=qry_result["directory"],
                        tree_last_modified=qry_result["last_modified"],
                        smb_write_lock_last_seen=qry_result["last_smb_write_lock"]))

            return result

    def get_work_item(self, source_dir, validate_pre_conditions=True):
        """
        :type source_dir: str
        :type ctime: datetime.datetime
        :rtype: Phase1WorkItem
        """

        if validate_pre_conditions:
            assert os.path.exists(source_dir), \
                "Phase1 Db was requested to search for a dir:{0} that does not exist on fs".format(source_dir)

        logging.debug("About to perform search for {0}".format(source_dir))

        params = (source_dir, )
        query = 'SELECT * FROM phase1_work_items WHERE directory=?'

        result = SqliteFacade.execute_parameterized_select(self._data_file_path, query, params)

        if result:
            assert len(result) == 1, "Phase1 Db is corrupted, only one record should exist per dir_name, ctime combo"
            phase1_work_item = Phase1WorkItem(
                source_dir=result[0]["directory"],
                tree_last_modified=result[0]["last_modified"],
                smb_write_lock_last_seen=result[0]["last_smb_write_lock"])
            return phase1_work_item

    def remove_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        """
        with sqlite3.connect(self._data_file_path) as connection:
            cursor = connection.cursor()
            sql_delete_query = "DELETE FROM phase1_work_items WHERE directory = ?"
            params = (phase1_work_item.phase1_source_dir,)
            cursor.execute(sql_delete_query, params)

    def clear_work_items(self):
        with sqlite3.connect(self._data_file_path,
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            sql_delete_query = "DELETE FROM phase1_work_items"
            cursor.execute(sql_delete_query)
