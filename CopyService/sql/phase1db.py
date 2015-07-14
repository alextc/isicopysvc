__author__ = 'alextc'
import os
import datetime
import logging
from datetime import timedelta
from model.phase1workitem import Phase1WorkItem
from log.loggerfactory import LoggerFactory
from sqlitefacade import SqliteFacade


class Phase1Db:
    _data_file_path = "/ifs/copy_svc/phase1.db"
    _logger = LoggerFactory().create('Phase1Db')

    def __init__(self):
        if not os.path.exists(os.path.dirname(Phase1Db._data_file_path)):
            raise ValueError("Directory does not exist '{0}'".format(os.path.dirname(Phase1Db._data_file_path)))

        command = "CREATE TABLE IF NOT EXISTS phase1_work_items " \
                  "(directory TEXT NOT NULL, " \
                  "birth_time timestamp NOT NULL, " \
                  "last_modified timestamp NOT NULL, " \
                  "last_smb_write_lock timestamp NOT NULL," \
                  " PRIMARY KEY (directory, birth_time))"

        SqliteFacade.execute_command(Phase1Db._data_file_path, command)

    def add_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        :return:
        """

        assert os.path.exists(phase1_work_item.phase1_source_dir), \
            "Directory does not exist {0}".format(phase1_work_item.phase1_source_dir)

        sql_insert_query = 'INSERT INTO phase1_work_items ' \
                           '(directory, birth_time, last_modified, last_smb_write_lock ) VALUES (?,?,?,?)'
        parameters = (phase1_work_item.phase1_source_dir,
                      phase1_work_item.birth_time,
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
                           'SET last_modified = ?, last_smb_write_lock= ? WHERE directory = ? AND birth_time = ?'

        parameters = (phase1_work_item.tree_last_modified,
                      phase1_work_item.last_smb_write_lock,
                      phase1_work_item.phase1_source_dir,
                      phase1_work_item.birth_time)

        SqliteFacade.execute_parameterized_command(self._data_file_path, sql_insert_query, parameters)

    def get_still_work_items(self, stillness_threshold_in_sec):
        result = []

        threshold = datetime.datetime.now() - timedelta(seconds=stillness_threshold_in_sec)
        logging.debug("Calculated threshold {0}".format(threshold))

        command = 'SELECT * FROM phase1_work_items WHERE last_smb_write_lock < ?'
        parameters = (threshold,)
        qry_results = SqliteFacade.execute_parameterized_select(self._data_file_path, command, parameters)

        if not qry_results:
            return

        result = []
        for qry_result in qry_results:
            result.append(Phase1WorkItem(
                source_dir=qry_result["directory"],
                birth_time=qry_result["birth_time"],
                tree_last_modified=qry_result["last_modified"],
                smb_write_lock_last_seen=qry_result["last_smb_write_lock"]))

        return result

    def get_all_work_items(self):
        command = 'SELECT * FROM phase1_work_items'
        qry_results = SqliteFacade.execute_select(self._data_file_path, command)

        if not qry_results:
            return

        result = []
        for qry_result in qry_results:
            result.append(
                Phase1WorkItem(
                    source_dir=qry_result["directory"],
                    birth_time=qry_result["birth_time"],
                    tree_last_modified=qry_result["last_modified"],
                    smb_write_lock_last_seen=qry_result["last_smb_write_lock"]))

        return result

    def get_work_item(self, source_dir, birth_time, validate_pre_conditions=True):
        """
        :type source_dir: str
        :rtype: Phase1WorkItem
        """

        if validate_pre_conditions:
            assert os.path.exists(source_dir), \
                "Phase1 Db was requested to search for a dir:{0} that does not exist on fs".format(source_dir)

        logging.debug("About to perform search for {0}".format(source_dir))

        params = (source_dir, birth_time )
        query = 'SELECT * FROM phase1_work_items WHERE directory = ? AND birth_time = ?'

        result = SqliteFacade.execute_parameterized_select(self._data_file_path, query, params)

        if result:
            assert len(result) == 1, "Phase1 Db is corrupted, only one record should exist per dir_name, ctime combo"
            phase1_work_item = Phase1WorkItem(
                source_dir=result[0]["directory"],
                birth_time=result[0]["birth_time"],
                tree_last_modified=result[0]["last_modified"],
                smb_write_lock_last_seen=result[0]["last_smb_write_lock"])
            return phase1_work_item

    def remove_work_item(self, phase1_work_item):
        """
        :type phase1_work_item: Phase1WorkItem
        """
        command = "DELETE FROM phase1_work_items WHERE directory = ? and birth_time = ?"
        parameters = (phase1_work_item.phase1_source_dir, phase1_work_item.birth_time)
        SqliteFacade.execute_parameterized_command(self._data_file_path, command, parameters)

    def clear_work_items(self):
        command = "DELETE FROM phase1_work_items"
        SqliteFacade.execute_command(self._data_file_path, command)
