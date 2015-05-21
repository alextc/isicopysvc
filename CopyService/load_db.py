''' Load sqlite DB with results of treewalks'''
import datetime as D
import socket
import sqlite3
import sys
import os
import fnmatch
import re
import decimal
# coding:UTF-8

datestamp = D.datetime.now().strftime("%Y%m%d")

# Get cluster's name
clustername = socket.gethostname().rsplit('-', 1)[0]

# Set up paths
BASEDIR     = "/ifs/data/Isilon_Support/trending"
SHARES_LIST = BASEDIR + "/" + clustername \
              + "_shared_dirs_" + datestamp + ".txt"
TREEDIR     = BASEDIR + "/trees/"
REPORTDIR   = BASEDIR + "/reports/"

# Code DB name
database_name = BASEDIR + "/trending.db"

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

# Start work here
# Build list of shares
shares = []
with open(SHARES_LIST) as list_of_shares:
    for each_share in list_of_shares:
        each_share = each_share.rstrip()
        if os.path.exists(each_share):
        ######## TESTING PURPOSES #################
        #if os.path.exists(TREEDIR + each_share):
        ###########################################
            shares.append(each_share)

all_files = []
for found in find('20??????', TREEDIR):
    statinfo = os.stat(found)
    filesize = statinfo.st_size
    if filesize > 0:
        all_files.append(found)

conn = sqlite3.connect(database_name)
c = conn.cursor()
c.execute("PRAGMA synchronous=OFF")
c.execute("PRAGMA cache_size=8000")

c.execute('''CREATE TABLE if not exists tbl 
            (path TEXT NOT NULL, 
             atime INT(10) NOT NULL,
             size INT NOT NULL, 
             name TEXT NOT NULL)''')
c.execute("CREATE INDEX if not exists index_tbl on tbl (path, atime)")
conn.commit()

sql_insert_query = "INSERT INTO tbl VALUES (?,?,?,?)"
c.execute("BEGIN TRANSACTION")

#1407513444|4096|/ifs/data/DCP/DPW/PubTransfer/M-R/._MARVEL PRESS
a_line = re.compile(r'(?P<atime>\d+)\|(?P<size>\d+)\|(?P<path>.*)')

for each_file in all_files:
    file = open(each_file, 'rt')
    while 1:
        each_line = file.readline()
        if not each_line:
            break
        else:
            line = each_line.rstrip('\n')
            regexes = re.search(a_line, line)
            if regexes:
                atime = regexes.group('atime')
                size = regexes.group('size')
                thefullpath = regexes.group('path')
                thepath, thename = thefullpath.rsplit('/', 1)
                if re.match(r'\d+', atime) \
                    and re.match(r'\d+', size) \
                    and re.match(r'/ifs/.+', thefullpath):
                    path = thepath.decode('utf-8')
                    name = thename.decode('utf-8')
                    c.execute(sql_insert_query, (path, atime, size, name))
                else:
                    pass
    file.close()
conn.commit()


# Create table for each and every share
# Yes is duplicating, but, results in quicker queries
i = 1
for each_path in shares:
    # Create 'like' part of string to use for sql queries
    like_share = each_path + "%"
    # Make acceptable table name from share path
    shared_path1 = each_path.replace('/', '_')
    shared_path  = shared_path1.replace('-', '_dash_')
    q_create1 = "CREATE TABLE if not exists %s " % shared_path
    q_create2 = ''' (path TEXT NOT NULL,
                     atime INT(10) NOT NULL,
                     size INT NOT NULL,
                     name TEXT NOT NULL )'''
    create_query = q_create1 + q_create2
    c.execute(create_query)
    index  = "index_" + str(i)
    z_tbl = shared_path
    q_index = "CREATE INDEX if not exists %s on %s (atime)" % (index, z_tbl)
    c.execute(q_index)
    i += 1
    conn.commit()

    # COPY for each share all data from tbl in its path
    #  over to new table named like _share_path_like_this
    q_copy1 = "INSERT INTO %s " % shared_path
    q_copy2 = "SELECT * FROM tbl "
    q_copy3 = "WHERE path like '%s'" % like_share
    copy_query = q_copy1 + q_copy2 + q_copy3
    c.execute("BEGIN TRANSACTION")
    c.execute(copy_query)
    conn.commit()

conn.commit()
conn.close()
