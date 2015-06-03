__author__ = 'alextc'
import sqlite3
import os
from datetime import datetime

class HeartBeatDb:
    def __init__(self, data_file_path):
        if not os.path.exists(os.path.dirname(data_file_path)):
            raise ValueError("Directory does not exist '{}'".format(os.path.dirname(data_file_path)))

        self._data_file_path = data_file_path
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='heartbeats'")
            result = cursor.fetchone();
            if not result:
                cursor.execute("CREATE TABLE heartbeats (heartbeat timestamp NOT NULL)")

    def write_heart_beat(self):
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            now = datetime.now()
            cursor.execute("DELETE from heartbeats")
            cursor.execute('INSERT into heartbeats (heartbeat) VALUES (?)', (now,))

    def get_heart_beat(self):
        with sqlite3.connect(self._data_file_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM heartbeats')
            result = cursor.fetchone()

        if result:
            return result[0]

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


