import unittest
import os
import sqlite3

class SqliteWrapper:
    
    def __init__(self, data_file_path):
        if not os.path.exists(os.path.dirname(data_file_path)):
            raise ValueError("Directory does not exist '{}'".format(os.path.dirname))

        self._data_file_path = data_file_path
        self._connection = sqlite3.connect(data_file_path)
        cursor = self._connection.cursor()
        cursor.execute('''CREATE TABLE if not exists openfiles
                          (directory TEXT NOT NULL PRIMARY KEY,
                           last_seen INT NOT NULL)''')
        self._connection.commit()
        self._connection.close()

    def update_last_seen(self, path, date):
        sql_insert_query = 'INSERT or REPLACE into openfiles (directory,last_seen) VALUES (?,?)'
        cursor = self._connection.cursor()
        cursor.execute(sql_insert_query, (path, date))
        conn.commit()
        conn.close()

    def get_last_seen(self, path):
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM last_seen WHERE directory=?', path)
        return cursor.fetchone()

    def delete_last_seen_records(self, path):
        cursor =  conn.cursor()
        sql_delete_query = "DELETE from openfiles WHERE directory = ?"
        cursor.execute(sql_delete_query, [path])
        self._connection.commit()
        self._connection.close()

    def drop_last_seen_table(self):
        cursor = conn.cursor()
        sql = "drop table openfiles"
        cursor.execute(sql)
        self._connection.commit()
        self._connection.close()
        
class sqlite_wrapper_tests(unittest.TestCase):
    def test_init_must_create_new_db_file(self):
        db_path = "/ifs/copy_svc/files.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        wrapper = SqliteWrapper(db_path)
        self.assertTrue(os.path.exists(db_path))

if __name__ == '__main__':
    unittest.main()
