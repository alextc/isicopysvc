__author__ = 'alextc'
import sqlite3
import os
from model.phase2workitem import Phase2WorkItem
import logging


class HeartBeatDb:
    def __init__(self):
        self._data_file_path = "/ifs/copy_svc/heartbeat.db"
        with sqlite3.connect(
                self._data_file_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='heartbeats'")
            result = cursor.fetchone()
            if not result:
                cursor.execute("CREATE TABLE heartbeats "
                               "(directory TEXT NOT NULL, "
                               "directory_last_modified timestamp NOT NULL, "
                               "host TEXT NOT NULL, "
                               "pid TEXT NOT NULL, "
                               "heartbeat timestamp NOT NULL, "
                               "state TEXT NOT NULL, "
                               "PRIMARY KEY (directory, directory_last_modified))")

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

        try:
            with sqlite3.connect(
                    self._data_file_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                cursor = connection.cursor()
                cursor.execute \
                    ('INSERT INTO heartbeats (directory, directory_last_modified, host, pid, heartbeat, state) '
                     'VALUES (?,?,?,?,?,?)',
                     (work_item.phase2_source_dir,
                      work_item.phase2_source_dir_last_modified,
                      work_item.host,
                      work_item.pid,
                      work_item.heartbeat,
                      work_item.state))
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
        try:
            with sqlite3.connect(
                    self._data_file_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                cursor = connection.cursor()
                cursor.execute('DELETE FROM heartbeats '
                               'WHERE directory=(?) AND directory_last_modified=(?) AND host=(?) AND pid=(?)',
                               (work_item.phase2_source_dir,
                                work_item.phase2_source_dir_last_modified,
                                work_item.host,
                                work_item.pid))
        except sqlite3.IntegrityError as e:
            logging.debug(e)
            raise

    def write_heart_beat(self, work_item):
        """
        :type work_item: Phase2WorkItem
        :return:
        """
        logging.debug("ENTERING")
        logging.debug("Received work_item to write:\n{0}".format(work_item))
        try:

            with sqlite3.connect(
                    self._data_file_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                cursor = connection.cursor()
                cursor.execute ('INSERT OR REPLACE INTO heartbeats '
                                '(directory, directory_last_modified, host, pid, heartbeat, state)'
                                ' VALUES (?,?,?,?,?,?)',
                                (work_item.phase2_source_dir,
                                 work_item.phase2_source_dir_last_modified,
                                 work_item.host,
                                 work_item.pid,
                                 work_item.heartbeat,
                                 work_item.state))
        except sqlite3.Error as e:
            logging.debug(e.message)
            raise

    # TODO: should not this be directory and the last_modified - otherwise this may return multiple items
    def get_heart_beat(self, directory):
        """
        :type directory: str
        :rtype: Phase2WorkItem
        """
        with sqlite3.connect(
                self._data_file_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM heartbeats WHERE directory = (?)', (directory,))
            result = cursor.fetchone()

        # no matching record found
        if not result:
            logging.debug("get_heart_beat did not return anything")
            return None

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
        with sqlite3.connect(
                self._data_file_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            sql_delete_query = "DELETE from heartbeats"
            cursor.execute(sql_delete_query)
