import optparse
import isi.rest
import json
import sys
import datetime
import re


# Define what path should look like for copy_svc
# This will narrow down the paths of shares (shared rood paths) to look at
copy_svc_designator = re.compile(r"/ifs/MS", re.UNICODE)


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


# Grab command line option values, if any
options = process_command_line()

debug   = options.debug
verbose = options.verbose

if debug:
    print vars(options)


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
### What are we getting back from the PAPI call?  
  # The response to GET is a tuple.
  # example of a tuple returned
''' (200, {'status': '200 Ok', 'content-type': 'application/json', 'allow': 'GET, HEAD'}, '\n{\n"openfiles" : \n[\n\n{\n"file" : "C:\\\\ifs",\n"id" : 100,\n"work" : 0,\n"permissions" : [ "read" ],\n"user" : "root"\n},\n\n{\n"file" : "C:\\\\ifs",\n"id" : 488,\n"work" : 0,\n"permissions" : [ "read" ],\n"user" : "root"\n}\n],\n"resume" : null,\n"total" : 2\n}\n') '''

  # response[0] is an integer (int).  In this case 200.
  # response[1] is a dictionary.  In this case it is:   {'status': '200 Ok', 'content-type': 'application/json', 'allow': 'GET, HEAD'} 
  # response[2] is a string and in this case what we're interested in.  In this case it is:  
'''
    {
    "openfiles" :
    [

    {
    "file" : "C:\\ifs",
    "id" : 100,
    "work" : 0,
    "permissions" : [ "read" ],
    "user" : "root"
    },
    ...snipped...
    "resume" : null,
    "total" : 2
    }
''' 

    # Using json(loads) we can take that string returned as third element of tuple from GET and put right in to native Python dictionary.
    # The json.dumps() function takes a Python data structure and returns it as a JSON string.
    # The json.loads() function takes a JSON string and returns it as a Python data structure.

    # Similarly as with a successful response, if error returned it is the third element of the response that'll have the error info


def grab_smb_shares():
    url_parts = ['1', 'protocols', 'smb', 'shares']
    return papi_action('GET', url_parts)[2]


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



# Compile a regex for all shared root paths that we care about
share_compilation = ''

# Get all shares on system
json_shares_dict = json.loads(grab_smb_shares())
shares_list = json_shares_dict['shares']

for share in shares_list:
    test_copy_svc_shared_path = re.search(copy_svc_designator, share['path'])

    # If we have path we want, convert it to Win semantics e.g. flip the slashes
    if test_copy_svc_shared_path:
        path_in_list = share['path'][1:].split("/")
        root_dir = "C:"
        for each_directory in path_in_list:
            root_dir += r"\\%s" % each_directory

        if share_compilation:
            share_compilation += "|%s" % root_dir
        else:
            share_compilation = "%s" % root_dir

if debug:
    print "Regex compiled to match shares we are looking for:  %s" % share_compilation
desired_shared_path = re.compile(share_compilation, re.UNICODE)



# Get the time right now in epoch to be used to put in to db with entries
# And to be used for logging too. ???
epoch = int(datetime.datetime.now().strftime("%s"))


# Grab all open files 
# *** What happens when get back 100,000,000?  Do we have to handle multiple REST responses???
# ????????????????????????????????????????????????????????????????????????????????????????????
json_openfile_dict =  json.loads(grab_openfiles())

# Dictionary, json_openfile_dict has one key 'openfiles'
# That key's value is a list
openfiles_list = json_openfile_dict['openfiles']


# matched_files will hold files seen that we care about
matched_files = []



# Look at each entry returned and see if we need to keep it
for openfile in openfiles_list:
    if debug:
        print "We are working on matching the following:  %s" % openfile['file']

    # Each openfile is a dictionary with five keys:  work, user, id, file, permissions
    # Example:  {u'work': 0, u'user': u'root', u'id': 100, u'file': u'C:\\ifs', u'permissions': [u'read']}
    # We only care about open files with write permissions


    # If we have seen one file open in an interesting-to-us shared root path, skip
    # If this file is in share for copy_svc, and we have already seen it then move on
    # ........... Probably should alter order here and look for matching paths of files before writes...

    if 'write' in openfile['permissions']:
        if debug:
            print "This one has a write in permissions:   %s" % openfile['file']

        test_desired_share_path = re.search(desired_shared_path, openfile['file'])
        if test_desired_share_path:
            if debug:
                print "This one is also under a shared path that we are interested in:  %s" % openfile['file'] 
            path_seen = test_desired_share_path.group(0)
            sub_dir_seen = openfile['file'].replace(path_seen, '').split('\\')[1]
          
            # Add this path to a list to track since we do not care about any other open files that are under this path during this run
            if debug:
                print "Write seen in shared path %s" % path_seen
                print "Sub-dir where write seen %s" % sub_dir_seen
            
            #matched_files
      
        
    # If we need/should go ultra atomic, this info (sub-dir of shared root path) will be appended to a file
    # as well as put in to list with other sub-directories under shared root paths where we saw a write
    # Once we have gone through all open files we'll close the file then insert or update in to database
    # Also, should we log full path at this point?  Do not thing we want full path in DB, just the sub-dir under shared root path...



    
