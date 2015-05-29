__author__ = 'alextc'
import sqlite3
import os
from CopyService.Common.datetimewrapper import DateTimeWrapper

class WriteLockDb:

    def __init__(self, data_file_path):
        if not os.path.exists(os.path.dirname(data_file_path)):
            raise ValueError("Directory does not exist '{}'".format(os.path.dirname(data_file_path)))

        self._data_file_path = data_file_path
        with sqlite3.connect(self._data_file_path) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='openfiles'")
            result = cursor.fetchone();
            if not result:
                cursor.execute("CREATE TABLE openfiles (directory TEXT NOT NULL PRIMARY KEY, last_write_lock INT NOT NULL)")


    def insert_or_replace_last_seen(self, path, date):
        if not os.path.exists(path):
            raise ValueError("Directory does not exist '{}'".format(path))

        datetime_wrapper = DateTimeWrapper()
        if not datetime_wrapper.is_datetime_in_expected_format(date):
            raise ValueError("Supplied date was not in the expected format")

        sql_insert_query = 'INSERT or REPLACE into openfiles (directory,last_write_lock) VALUES (?,?)'
        with sqlite3.connect(self._data_file_path) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (path, date))

    def insert_or_replace_last_seen_ignore_if_exists(self, path, date):
        if not os.path.exists(path):
            raise ValueError("Directory does not exist '{}'".format(path))

        datetime_wrapper = DateTimeWrapper()
        if not datetime_wrapper.is_datetime_in_expected_format(date):
            raise ValueError("Supplied date was not in the expected format")

        sql_insert_query = 'INSERT or IGNORE into openfiles (directory,last_write_lock) VALUES (?,?)'
        with sqlite3.connect(self._data_file_path) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_insert_query, (path, date))


    def get_last_seen_record_for_dir(self, path):
        connection = sqlite3.connect(self._data_file_path)
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM openfiles WHERE directory=?', [path])
        result =  cursor.fetchone()
        connection.close()

        if result:
            return {'directory': result[0], 'last_write_lock': result[1]}

        return result

    def dump(self):
        connection = sqlite3.connect(self._data_file_path)
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM openfiles')
        results =  cursor.fetchall()
        connection.close()

        for result in results:
            print("{0} {1}".format(result[0], result[1]))

    def delete_last_seen_records(self, path):
        with sqlite3.connect(self._data_file_path) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from openfiles WHERE directory = ?"
            cursor.execute(sql_delete_query, [path])


    def clear_last_seen_table(self):
        with sqlite3.connect(self._data_file_path) as connection:
            cursor =  connection.cursor()
            sql_delete_query = "DELETE from openfiles"
            cursor.execute(sql_delete_query)

    def drop_last_seen_table(self):
        with sqlite3.connect(self._data_file_path) as connection:
            cursor = connection.cursor()
            sql = "drop table openfiles"
            cursor.execute(sql)


