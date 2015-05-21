#!/usr/bin/env python


''' insert in to DB '''

import sqlite3



# Set up paths
BASEDIR     = "/ifs/MS"

# Code DB name
database_name = BASEDIR + "/openfiles.db"



conn = sqlite3.connect(database_name)
c = conn.cursor()


c.execute('''CREATE TABLE if not exists openfiles
            (directory TEXT NOT NULL PRIMARY KEY,
             last_seen INT(10) NOT NULL)''')
conn.commit()

directory = 'asdf'
last_seen = 1432067210
sql_insert_query = 'INSERT or REPLACE into openfiles (directory,last_seen) VALUES (?,?)'
c.execute("BEGIN TRANSACTION")

c.execute(sql_insert_query, (directory, last_seen))



conn.commit()
conn.close()
