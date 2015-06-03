__author__ = 'alextc'
import sqlite3
from datetime import datetime

class HeartBeatDb:
    def __init__(self):
        self._data_file_path = "/ifs/copy_svc/heartbeat.db"
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='heartbeats'")
            result = cursor.fetchone();
            if not result:
                cursor.execute("CREATE TABLE heartbeats "
                               "(directory TEXT NOT NULL PRIMARY KEY, host TEXT NOT NULL, heartbeat timestamp NOT NULL)")

    def write_heart_beat(self, work_item):
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            now = datetime.now()
            cursor.execute('INSERT OR REPLACE into heartbeats (directory, host, heartbeat) VALUES (?,?,?)',
                           (work_item.source_dir, work_item.host, now,))

    def get_heart_beat(self, directory):
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM heartbeats WHERE directory = (?)', (directory,))
            result = cursor.fetchone()

        if result:
            return {'directory': result[0], 'host': result[1], 'heartbeat': result[2]}

    def dump(self):
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM heartbeats')
            results =  cursor.fetchall()

        for result in results:
            print("{0}".format(result[0]))

    def clear_heart_beat_table(self):
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from heartbeats"
            cursor.execute(sql_delete_query)


