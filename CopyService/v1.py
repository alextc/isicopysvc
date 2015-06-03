import glob
import unicodedata
import os
import sqlite3
import optparse
import shutil
import isi.rest
import json
import sys
import datetime
import re
import errno


trigger_interval_in_seconds = 60
persistence_file = "/ifs/copy_svc/persistence.file"
datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'
database = "/ifs/copy_svc/openfiles.db"
copy_svc_paths = glob.glob("/ifs/zones/*/copy_svc/to/*/*")
if copy_svc_paths:
    print copy_svc_paths


def flip_slashes():
    temp_list = []
    for each_element in copy_svc_paths:
        new_element = "C:" + each_element.replace("/", "\\")
        temp_list.append(new_element)
    return temp_list
        

def process_command_line():
    parser = optparse.OptionParser()
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False,
                      help="Print stuff along the way.")
    parser.add_option("-d", "--debug",
                      action="store_true", dest="debug", default=False,
                      help="Extreme verbosity mostly for debugging while writing.")

    (options, args) = parser.parse_args()

    # Don't care about arguments as not looking for any...
    if len(args) != 0:
        parser.error("No arguments allowed!")

    return(options)

def papi_action(action, url_parts, query_dict={}, header_dict={}, body_data='', timeout=120):
    response = isi.rest.send_rest_request(
        socket_path = isi.rest.PAPI_SOCKET_PATH,
        method = action,
        uri = url_parts,
        query_args = query_dict,
        headers = header_dict,
        body = body_data,
        timeout = timeout
    )
    return response


def grab_openfiles():
    url_parts = ['1', 'protocols', 'smb', 'openfiles']
    # NOTE:  Although definition for papi_action gives us whole tuple from GET, 
    # here we only put to use the third element (aka [2]) of the tuple which has everything we need
    return papi_action('GET', url_parts)[2]


def print_errors(response_json):
    if response_json.has_key('errors'):
        errors = response_json['errors']
        for e in errors:
            print e['message']


def drop_table():
    try:
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "drop table openfiles"
        c.execute(sql)
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print e.args[0]

def update_database(path, date, action):
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
    conn.commit()
    conn.close()

def delete_entry_from_database(path):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    sql_delete_query = "DELETE from openfiles WHERE directory = '" + path + "'"
    c.execute(sql_delete_query)
    conn.commit()
    conn.close()

 

def process_persistence_file():
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
                    print "We are working on matching the following:  %s" % openfile['file']

                # Each openfile is a dictionary with five keys:  work, user, id, file, permissions
                # Example:  {u'work': 0, u'user': u'root', u'id': 100, u'file': u'C:\\ifs', u'permissions': [u'read']}


                if 'write' in openfile['permissions']:
                    if debug:
                        print "This one has a write in permissions:   %s" % openfile['file']

                    print openfile['file']
                    for each_interesting in copy_svc_paths:
                        print each_interesting
                        if openfile['file'].startswith(each_interesting):
                            update_database(each_interesting, persistence_date, False)
                            copy_svc_paths.remove(each_interesting)
                            break
                    if not copy_svc_paths:
                        break
            if copy_svc_paths:
              for each_interesting in copy_svc_paths:
                  update_database(each_interesting, persistence_date, True)
                             
 


def delete_persistence_file():
    if os.path.isfile(persistence_file):
        os.remove(persistence_file)
        
        


def fill_persistence_file():
    try:
        open_files = grab_openfiles() 
        right_now = datetime.datetime.utcnow()
        if open_files:
            with open(persistence_file, 'w') as persistence:
                persistence.writelines(right_now.strftime(datetime_format_string))
                persistence.writelines(open_files)
    except IOError as e:
        print "in fill_persistence_file def"
        print e

def get_triggered_file():
    conn = sqlite3.connect(database)
    c = conn.cursor()
    sql_grab_diffs_query = "select * from openfiles where (strftime('%s', 'now') - strftime('%s', last_seen)) > " + str(trigger_interval_in_seconds)
    print sql_grab_diffs_query
    c.execute(sql_grab_diffs_query)
    return c.fetchall()
    conn.close()
     

def work_on_triggered_dirs(triggered_dirs):
    for each_triggered_dir in triggered_dirs:
        dir_to_move = each_triggered_dir[0].split(':')[1].replace("\\", "/")
        dir_parts = dir_to_move.split('/')
        print dir_parts
        target_dir = "/" + dir_parts[1] + "/" + dir_parts[2] + "/" + dir_parts[6] + "/copy_svc/staging/" + dir_parts[3] + "/" + dir_parts[7]
        parent_target_dir = "/" + dir_parts[1] + "/" + dir_parts[2] + "/" + dir_parts[6] + "/copy_svc/staging/" + dir_parts[3] 
        if not os.path.exists(parent_target_dir):
            print parent_target_dir
            os.mkdir(parent_target_dir)
        if os.path.exists(dir_to_move):
            shutil.move(dir_to_move, target_dir)
        normalized = unicodedata.normalize('NFKD', each_triggered_dir[0]).encode('ascii', 'ignore')
        print type(normalized)
        print normalized

        delete_entry_from_database(normalized)
        

def restart_script():
    pass

try:


    options = process_command_line()
    debug   = options.debug
    verbose = options.verbose

    if debug:
        print vars(options)

    copy_svc_paths = flip_slashes()

    if not copy_svc_paths:
        drop_table()
        delete_persistence_file()
        sys.exit("No work to do.")

    if not os.path.isfile(persistence_file):
        fill_persistence_file()

    process_persistence_file()

    delete_persistence_file()

    triggered_dirs = get_triggered_file()

    if triggered_dirs:
        work_on_triggered_dirs(triggered_dirs)


finally:
    restart_script()    
