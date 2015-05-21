#!/usr/bin/env python


''' delete '''

import sqlite3



# Set up paths
BASEDIR     = "/ifs/MS"

# Code DB name
database_name = BASEDIR + "/openfiles.db"



conn = sqlite3.connect(database_name)
c = conn.cursor()

directory = "asdf"
delete_directories = 'DELETE from openfiles WHERE directory = "%s"' % directory
c.execute(delete_directories)


conn.commit()
conn.close()
