__author__ = 'alextc'
import sqlite3
import os
from model.phase2workitem import Phase2WorkItem
from sqlitefacade import SqliteFacade
import logging


class Phase2Db:
    def __init__(self):
        self._data_file_path = "/ifs/copy_svc/phase2.db"

        command = "CREATE TABLE IF NOT EXISTS phase2_work_items " \
                  "(directory TEXT NOT NULL, " \
                  "directory_last_modified timestamp NOT NULL, " \
                  "host TEXT NOT NULL, " \
                  "pid TEXT NOT NULL, " \
                  "heartbeat timestamp NOT NULL, " \
                  "state TEXT NOT NULL, " \
                  "PRIMARY KEY (directory, directory_last_modified))"

        SqliteFacade.execute_command(self._data_file_path, command)

    def try_to_take_ownership(self, work_item):
        """
        :type work_item: Phase2WorkItem
        :return:
        """
        logging.debug("ENTERING")
        # logging.debug("Received work_item to take ownership:\n{0}".format(work_item))

        if not os.path.exists(work_item.phase2_source_dir):
            logging.debug("try_to_take_ownership was called on path {0} that does not exist.".format(
                work_item.phase2_source_dir))
            logging.debug("Assuming that somebody else already processed and clean-up this directory.")
            logging.debug("Returning False")
            return False

        command = 'INSERT INTO phase2_work_items (directory, directory_last_modified, host, pid, heartbeat, state) ' \
                  'VALUES (?,?,?,?,?,?)'

        parameters = (work_item.phase2_source_dir,
                      work_item.phase2_source_dir_last_modified,
                      work_item.host,
                      work_item.pid,
                      work_item.heartbeat,
                      work_item.state)

        try:
            SqliteFacade.execute_parameterized_command(self._data_file_path, command, parameters)
        except sqlite3.IntegrityError as e:
            logging.debug(e)
            return False

        return True

    def remove_work_item(self, work_item):
        """
        :type work_item: Phase2WorkItem
        :return:
        """
        logging.debug("ENTERING remove_work_item")
        # logging.debug("Relieved work_item to remove:\n{0}".format(work_item))

        command = 'DELETE FROM phase2_work_items ' \
                  'WHERE directory=(?) AND directory_last_modified=(?) AND host=(?) AND pid=(?)'

        parameters = (work_item.phase2_source_dir,
                      work_item.phase2_source_dir_last_modified,
                      work_item.host,
                      work_item.pid)

        SqliteFacade.execute_parameterized_command(self._data_file_path, command, parameters)

    def write_heart_beat(self, work_item):
        """
        :type work_item: Phase2WorkItem
        :return:
        """
        logging.debug("ENTERING")
        logging.debug("Received work_item to write:\n{0}".format(work_item))

        command = 'INSERT OR REPLACE INTO phase2_work_items ' \
                  '(directory, directory_last_modified, host, pid, heartbeat, state) VALUES (?,?,?,?,?,?)'

        parameters = (work_item.phase2_source_dir,
                      work_item.phase2_source_dir_last_modified,
                      work_item.host,
                      work_item.pid,
                      work_item.heartbeat,
                      work_item.state)

        SqliteFacade.execute_parameterized_command(self._data_file_path, command, parameters)

    # TODO: should not this be directory and the last_modified - otherwise this may return multiple items
    def get_heart_beat(self, directory):
        """
        :type directory: str
        :rtype: Phase2WorkItem
        """

        query = 'SELECT * FROM phase2_work_items WHERE directory = (?)'
        parameters = (directory,)

        result = SqliteFacade.execute_parameterized_select(self._data_file_path, query, parameters)

        # no matching record found
        if not result:
            logging.debug("get_heart_beat did not return anything")
            return None

        assert len(result) == 1, "Phase2 Db is corrupted, only one record should exist per dir_name"

        heart_beat = Phase2WorkItem(
            phase2_source_dir=result["directory"],
            phase2_source_dir_last_modified=result["directory_last_modified"],
            state=result["state"],
            heartbeat=result["heartbeat"])
        heart_beat.pid = result["pid"]
        heart_beat.host = result["host"]

        logging.debug("get_heart_beat returning {0}".format(heart_beat))
        return heart_beat

    def clear_heart_beat_table(self):
        command = "DELETE from phase2_work_items"
        SqliteFacade.execute_command(self._data_file_path, command)
