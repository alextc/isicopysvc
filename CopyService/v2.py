#!/usr/bin/env python
""" Finds directories to move."""

import glob
import optparse
import sqlite3
import os
import sys
import json
import unicodedata
import shutil
import datetime
import time
import Common.Logging as Logger
import Common.papi as PAPI


trigger_interval_in_seconds    = 10
time_to_sleep_between_restarts = 1
datetime_format_string         = '%Y, %m, %d, %H, %M, %S, %f'
database                       = "/ifs/copy_svc/openfiles.db"
persistence_file               = "/ifs/copy_svc/persistence.file"
copy_svc_paths                 = glob.glob("/ifs/zones/*/copy_svc/to/*/*")


def process_command_line():
    Logger.log_debug("ENTER process_command_line")
    my_ret = []
    parser = optparse.OptionParser()
    parser.add_option("-d", "--debug",
                      action="store_true", dest="debug", default=False,
                      help="Extreme verbosity mostly for debugging while writing.")
    (options, args) = parser.parse_args()
    my_ret = options

    # Don't care about arguments as not looking for any...
    if len(args) != 0:
        parser.error("No arguments allowed!")

    Logger.log_debug(" EXIT process_command_line " + str(my_ret))
    return(options)

def flip_slashes():
    Logger.log_debug("ENTER flip_slashes")
    my_ret = []
    for each_element in copy_svc_paths:
        new_element = "C:" + each_element.replace("/", "\\")
        my_ret.append(new_element)
    Logger.log_debug(" EXIT flip_slashes " + str(my_ret))
    return my_ret

def drop_table():
    Logger.log_debug("ENTER drop_table")
    my_ret = False
    try:
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "drop table openfiles"
        c.execute(sql)
        conn.commit()
        conn.close()
        my_ret = True
    except sqlite3.Error as e:
        pass
    Logger.log_debug(" EXIT drop_table " + str(my_ret))
    return my_ret

def delete_persistence_file():
    Logger.log_debug("ENTER delete_persistence_file")
    my_ret = False
    if os.path.isfile(persistence_file):
        my_ret = os.remove(persistence_file)
    Logger.log_debug(" EXIT delete_persistence_file " + str(my_ret))
    return my_ret
        
def fill_persistence_file():
    Logger.log_debug("ENTER fill_persistence_file")
    my_ret = False
    try:
        open_files = PAPI.grab_smbopenfiles() 
        right_now = datetime.datetime.utcnow()
        if open_files:
            with open(persistence_file, 'w') as persistence:
                persistence.writelines(right_now.strftime(datetime_format_string))
                my_ret = persistence.writelines(open_files)
    except IOError as e:
        if debug:
            print "      DEBUG:  IOError while in fill_persistence_file function" % e.args[0]
    Logger.log_debug(" EXIT fill_persistence_file " + str(my_ret))
    return my_ret

def process_persistence_file():
    Logger.log_debug("ENTER process_persistence_file")
    my_ret = False
    with open(persistence_file) as persistence:
        persistence_string = persistence.readline().strip()
        if persistence_string:
            persistence_date = datetime.datetime.strptime(persistence_string, datetime_format_string)
            other_lines = "".join(persistence.readlines())
            json_openfile_dict = json.loads(other_lines)

            # Dictionary, json_openfile_dict has one key 'openfiles'
            # That key's value is a list
            openfiles_list = json_openfile_dict['openfiles']

            # Look at each entry returned and see if we need to keep it
            for openfile in openfiles_list:
                if debug:
                    print "      DEBUG:  We are working on matching the following:  %s" % openfile['file']

                # Each openfile is a dictionary with five keys:  locks, user, id, file, permissions
                # Example:  {u'locks': 0, u'user': u'root', u'id': 100, u'file': u'C:\\ifs', u'permissions': [u'read']}


                if 'write' in openfile['permissions']:
                    if debug:
                        print "      DEBUG:  This one has a write in permissions:   %s" % openfile['file']

                    print openfile['file']
                    for each_interesting in copy_svc_paths:
                        if debug:
                            print "      DEBUG:  Interesting paths:  %s" % each_interesting
                        if openfile['file'].startswith(each_interesting):
                            update_database(each_interesting, persistence_date, False)
                            copy_svc_paths.remove(each_interesting)
                            break
                    if not copy_svc_paths:
                        break
            if copy_svc_paths:
              for each_interesting in copy_svc_paths:
                  my_ret = update_database(each_interesting, persistence_date, True)
    Logger.log_debug(" EXIT process_persistence_file " + str(my_ret))
    return my_ret
                             
def update_database(path, date, action):
    Logger.log_debug("ENTER update_database")
    my_ret = False
    try:
        conn = sqlite3.connect(database)
        c = conn.cursor()
        c.execute('''CREATE TABLE if not exists openfiles
                 (directory TEXT NOT NULL PRIMARY KEY,
                  last_seen INT NOT NULL)''')
        conn.commit()
        sql_insert_query = 'INSERT or REPLACE into openfiles (directory,last_seen) VALUES (?,?)'
        if action:
            sql_insert_query = 'INSERT or IGNORE into openfiles (directory,last_seen) VALUES (?,?)'
        c.execute(sql_insert_query, (path, date))
        my_ret = conn.commit()
        conn.close()
    except sqlite3.Error as e:
        if debug:
            print "      DEBUG:  sqlite3 error %s" % e.args[0]
    Logger.log_debug(" EXIT update_database " + str(my_ret))
    return my_ret

def get_triggered_files():
    Logger.log_debug("ENTER get_triggered_files")
    my_ret = []
    try:
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql_grab_diffs_query = "select * from openfiles where (strftime('%s', 'now') - strftime('%s', last_seen)) > " + str(trigger_interval_in_seconds)
        c.execute(sql_grab_diffs_query)
        my_ret = c.fetchall()
        conn.close()
    except sqlite3.Error as e:
        if debug:
            print "      DEBUG:  sqlite3 error %s" % e.args[0]
    Logger.log_debug(" EXIT get_triggered_files " + str(my_ret))
    return my_ret 
     
def delete_entry_from_database(path):
    Logger.log_debug("ENTER delete_entry_from")
    my_ret = False
    try:
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql_delete_query = "DELETE from openfiles WHERE directory = '" + path + "'"
        c.execute(sql_delete_query)
        conn.commit()
        my_ret = conn.close()
    except sqlite3.Error as e:
        if debug:
            print "      DEBUG:  sqlite3 error %s" % e.args[0]
    Logger.log_debug(" EXIT delete_entry_from " + str(my_ret))
    return my_ret

def work_on_triggered_dirs(triggered_dirs):
    Logger.log_debug("ENTER work_on_triggered")
    if debug:
        print "      DEBUG:  Need to work on %s" % triggered_dirs
#    my_ret = False
    for each_triggered_dir in triggered_dirs:
        dir_to_move = each_triggered_dir[0].split(':')[1].replace("\\", "/")
        if debug:
            print "      DEBUG:  dir_to_move:%s" % dir_to_move
        dir_parts = dir_to_move.split('/')
        target_dir = "/" + dir_parts[1] + "/" + dir_parts[2] + "/" + dir_parts[6] + "/copy_svc/staging/" + dir_parts[3] + "/" + dir_parts[7]
        if debug:
            print "      DEBUG:  target_dir:%s" % target_dir
        parent_target_dir = "/" + dir_parts[1] + "/" + dir_parts[2] + "/" + dir_parts[6] + "/copy_svc/staging/" + dir_parts[3] 
        if debug:
            print "      DEBUG:  parent_target_dir:%s" % parent_target_dir

        if not os.path.exists(parent_target_dir):
            if debug:
                print "      DEBUG:  need to create parent_target_dir since not exists"
            os.mkdir(parent_target_dir)

        if os.path.exists(dir_to_move):
            if debug:
                print "      DEBUG:  dir_to_move exists, so move it"
            try:
                shutil.move(dir_to_move, parent_target_dir)
                #shutil.move(dir_to_move, target_dir)
            except Error as e:
                if debug:
                    print "      DEBUG:  Error moving file:OSError:%s" % e.args[0]
                #pass  

        # Normalize before putting in to db
        normalized = unicodedata.normalize('NFKD', each_triggered_dir[0]).encode('ascii', 'ignore')
        if debug:
            print "      DEBUG:  normalizing path to:%s" % normalized
        if debug:
            print "      DEBUG:  deleting entry from database now" 
        my_ret = delete_entry_from_database(normalized)
        if not my_ret:
            if debug:
                print "      DEBUG:  something went wrong with delete"
                sys.exit(my_ret)
    Logger.log_debug(" EXIT work_on_triggered " + str(my_ret))
    return my_ret
        
def restart_script():
    Logger.log_debug("ENTER restart_script")
    my_ret = False
    if debug:
        print "      DEBUG:  sleeping for %s seconds" % time_to_sleep_between_restarts
    time.sleep(time_to_sleep_between_restarts)
    my_ret = os.execv(__file__, sys.argv)

    # This should never print...
    Logger.log_debug(" EXIT restart_script " + str(my_ret))

if __name__ == '__main__':
    try:
        options = process_command_line()
        debug   = options.debug

        if not copy_svc_paths:
            drop_table()
            delete_persistence_file()
            sys.exit("No work to do.")
        else:
            copy_svc_paths = flip_slashes()

        if not os.path.isfile(persistence_file):
            fill_persistence_file()

        process_persistence_file()

        delete_persistence_file()

        triggered_dirs = get_triggered_files()

        if triggered_dirs:
            work_on_triggered_dirs(triggered_dirs)

    finally:
        restart_script()    
