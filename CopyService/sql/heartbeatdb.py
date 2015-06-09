__author__ = 'alextc'
import sqlite3
from datetime import datetime
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
            result = cursor.fetchone();
            if not result:
                cursor.execute("CREATE TABLE heartbeats "
                               "(directory TEXT NOT NULL PRIMARY KEY, "
                               "host TEXT NOT NULL, "
                               "pid TEXT NOT NULL, "
                               "heartbeat timestamp NOT NULL,"
                               " state TEXT NOT NULL)")

    def try_to_take_ownership(self, work_item):
        logging.debug("ENTERING")
        # logging.debug("Recieved work_item to take ownership:\n{0}".format(work_item))
        try:
            with sqlite3.connect(
                    self._data_file_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                cursor = connection.cursor()
                now = datetime.now()
                cursor.execute \
                    ('INSERT INTO heartbeats (directory, host, pid, heartbeat, state) VALUES (?,?,?,?,?)',
                     (work_item.phase2_source_dir, work_item.host, work_item.pid, now, work_item.state))
        except sqlite3.IntegrityError as e:
            logging.debug(e)
            return False

        return True

    def remove_work_item(self, work_item):
        logging.debug("ENTERING remove_work_item")
        # logging.debug("Recieved work_item to remove:\n{0}".format(work_item))
        try:
            with sqlite3.connect(
                    self._data_file_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                cursor = connection.cursor()
                cursor.execute('DELETE FROM heartbeats WHERE directory = (?)', (work_item.phase2_source_dir,))
        except sqlite3.IntegrityError as e:
            logging.debug(e)
            return False

        return True

    def write_heart_beat(self, work_item):
        logging.debug("ENTERING")
        # logging.debug("Recieved work_item to write:\n{0}".format(work_item))
        try:

            with sqlite3.connect(
                    self._data_file_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                cursor = connection.cursor()
                now = datetime.now()
                cursor.execute \
                    ('INSERT OR REPLACE INTO heartbeats (directory, host, pid, heartbeat, state) VALUES (?,?,?,?,?)',
                     (work_item.phase2_source_dir, work_item.host, work_item.pid, now, work_item.state))
        except sqlite3.Error as e:
            logging.debug(e.message)
            raise

    def get_heart_beat(self, directory):
        with sqlite3.connect(
                self._data_file_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM heartbeats WHERE directory = (?)', (directory,))
            result = cursor.fetchone()

        if result:
            return Phase2WorkItem(result[0], result[4], result[3])

    def dump(self):
        with sqlite3.connect(
                self._data_file_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM heartbeats')
            results = cursor.fetchall()

        heart_beats = []
        for result in results:
            heart_beats.append(Phase2WorkItem(result[0], result[4], result[3]))

        return heart_beats

    def clear_heart_beat_table(self):
        with sqlite3.connect(
                self._data_file_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            sql_delete_query = "DELETE from heartbeats"
            cursor.execute(sql_delete_query)
