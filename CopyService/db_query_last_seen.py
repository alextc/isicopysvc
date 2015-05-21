#!/usr/bin/env python


''' query '''

import sqlite3



# Set up paths
BASEDIR     = "/ifs/MS"

# Code DB name
database_name = BASEDIR + "/openfiles.db"



conn = sqlite3.connect(database_name)
c = conn.cursor()

#last_seen                1432067210
trigger_if_older_than   = 1432088334
query_last = "SELECT * from openfiles WHERE 'last_seen' < %s" % trigger_if_older_than
c.execute(query_last)
for row in c.fetchall():
    
    # Each row is a tuple 
    print "directory %s, last seen write %s" % (row[0], row[1])


conn.commit()
conn.close()
