import os
import ConfigParser
import datetime
import logging
import subprocess
import sys
import re

CONFIG_FILE = "/ifs/copy_svc/config"
HEARTBEAT_LOG = '/ifs/copy_svc/Phase1Worker.log'
#HEARTBEAT_LOG = '/ifs/copy_svc/Phase1Worker.log.STALE'


def set_config():
    """
    Read configuration file
    :return: None
    """
    conf = ConfigParser.ConfigParser()
    try:
        with open(CONFIG_FILE) as f:
            conf.readfp(f)
    except IOError, i:
        raise i

    global LOOK_FOR_PHASE1, LOOK_FOR_PHASE2
    global PARENT_LOG_PATH, LOG_FILENAME, PATH_TO_LOG
    global STALE_HEARTBEAT, NUMBER_RUNNING_PHASE2

    STALE_HEARTBEAT = int(conf.get('Check_running', 'Stale'))
    NUMBER_RUNNING_PHASE2 = int(conf.get('Check_running', 'Running'))

    PARENT_LOG_PATH = conf.get('Check_running', 'LogPath')
    LOG_FILENAME = conf.get('Check_running', 'LogFile')
    PATH_TO_LOG = os.path.join(PARENT_LOG_PATH, LOG_FILENAME)
    logging.basicConfig(filename=PATH_TO_LOG, level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)d %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    LOOK_FOR_PHASE1 = conf.get('Check_running_one_node', 'Process')
    LOOK_FOR_PHASE2 = conf.get('Check_running_each_node', 'Process')

def get_date():
    """
    Get date and time in a specific format
    :return: Comma separated date and time
    """
    return datetime.datetime.now().strftime("%Y, %m, %d, %H, %M, %S")

def get_last_heartbeat():
    """
    Read last line of the log file
    :return: timestamp (2015, 06, 23, 06, 29, 43)
    """
    with open(HEARTBEAT_LOG, 'r') as f:
        this_heartbeat = f.readlines()[-1].rstrip()
        this_edited_heartbeat = this_heartbeat.split(',')[0].strip('[')
        a_edited_this_heartbeat = this_edited_heartbeat.replace(' ', ', ')
        b_edited_this_heartbeat = a_edited_this_heartbeat.replace('-', ', ')
        final_edited_this_heartbeat = b_edited_this_heartbeat.replace(':', ', ')
#####################################
#####################################
#        print "this_heartbeat is %s" % this_heartbeat
#        print "final edited heartbeat is %s" % final_edited_this_heartbeat
#####################################
#####################################
        return final_edited_this_heartbeat

def get_heartbeat_diff():
    """
    Difference in seconds
    :return: integer (seconds)
    """
    current = datetime.datetime.strptime(get_date(), '%Y, %m, %d, %H, %M, %S')
    last_record = datetime.datetime.strptime(get_last_heartbeat(), '%Y, %m, %d, %H, %M, %S')
    return (current - last_record).seconds

def get_processes(self):
    my_ret = {}

    # Populate keys with node names
    cmd_args = ['isi_for_array', '-s', 'date']
    get_nodes = subprocess.Popen(cmd_args, stdout=subprocess.PIPE)
    nodes_output = get_nodes.communicate()
    list_of_nodes_output = nodes_output[0].split('\n')
    for i in list_of_nodes_output:
        if i:
            node = i.split(':', 1)[0]
            my_ret[node] = {}
            my_ret[node]['process_output'] = []
            my_ret[node]['frequency'] = ''

    dont_want = ['isi_ropc', 'sh -c /usr/bin/python', 'isi_rdo', 'isi_for_array']
    cmd_args = ['isi_for_array', '-X',
                'ps', 'auxwww',
                '|', 'grep', self,
                '|', 'grep', '-v', 'grep']
    run_ps = subprocess.Popen(cmd_args, stdout=subprocess.PIPE)
    grepped_output = run_ps.communicate()
    list_of_grepped_output = grepped_output[0].split('\n')

    for i in list_of_grepped_output:
        if i and not i.endswith('exited with status 1'):
            node = i.split(' ')[0].strip(':')
            if not node in my_ret.keys():
                my_ret[node] = {}
                my_ret[node]['process_output'] = []
                my_ret[node]['frequency'] = ''
            my_ret[node]['process_output'].append(i)

    for a_node in my_ret.keys():
        _do_want = []
        for incr in range(0, len(my_ret[a_node]['process_output'])):
            if all(x not in my_ret[a_node]['process_output'][incr] for x in dont_want):
                _do_want.append(my_ret[a_node]['process_output'][incr])
        my_ret[a_node]['process_output'] = _do_want
        my_ret[a_node]['frequency'] = len(my_ret[a_node]['process_output'])

#    for asdf in my_ret.keys():
#        print asdf
#        print my_ret[asdf]['frequency']
#        for zxcv in my_ret[asdf]['process_output']:
#            print zxcv
    return my_ret

def kill_all(self):
    """
    Kill all the running processes
    :return: None
    """
    cmd = "isi_for_array ps auxwww|grep {0} |grep -v grep|".format(self) + r"awk '{print$3}'"
    process = subprocess.Popen(['/bin/bash', '-c', cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    communicate = filter(None, process.communicate()[0].split('\n'))
    null = open(os.devnull, 'w')

    for i in communicate:
        logging.info("Killing {0} now".format(i))
        subprocess.Popen(['/bin/bash', '-c', '/usr/bin/isi_for_array /bin/kill {0} '.format(i)], stderr=null,
              stdout=null).communicate()

def kill_this_pid(self, node):
    """ Given pid and node, kill it"""
    pass

def kill_em_all(self, pids):
    """ Kill all self on node"""
    nodes_lnn = self.rsplit('-', 1)[-1]
    for each_pid in pids:
        #logging...
        command = "isi_for_array -n %s kill %s" % (nodes_lnn, each_pid)
        subprocess.Popen(command.split())
    return

def start_phase1(self):
    """
    Start/Restart new process
    :return: None
    """
    logging.info("Starting {0} now".format(self))
    subprocess.Popen(['/bin/bash', '-c', '/usr/bin/isi_ropc /usr/bin/python {0} & disown'.format(self)])
    return

def start_phase2(self, need):
    nodes_lnn = self.rsplit('-', 1)[-1]
    logging.info("Need %s more processes on %s" % (need, self))
    for i in range(1, need+1):
        logging.info("Starting %s now on node %s" % (LOOK_FOR_PHASE2, self))
        command = "isi_for_array -n %s /usr/bin/python %s & disown" % (nodes_lnn, LOOK_FOR_PHASE2)
        subprocess.Popen(command.split())
    return

def get_pids(self):
    """Given list of lines of process output, return pids"""
    _list_of_pids = []
    for each_running_process in self:
        this_processes_pid = each_running_process.split(' ')[4]
        _list_of_pids.append(this_processes_pid)
    return _list_of_pids

#def test_ok(running, frequency, last_heartbeat, last_heartbeat_diff):
#    """
#    Check if the processing is running properly
#    :param running:
#    :param frequency:
#    :param last_heartbeat:
#    :param last_heartbeat_diff:
#    :return: None
#    """
#    if frequency == 1 and last_heartbeat_diff < int(STALE_HEARTBEAT):
#        logging.info('Found {0} with current heartbeat {1}'.format(LOOK_FOR, last_heartbeat))
#        logging.info(running[0])
#        print 'Test Passed'
#        sys.exit(0)


def main():
    set_config()


#    # Phase1 first, we only want one instance running on one node
#    # -----------------------------------------------------------
#    dict_processes_phase1 = get_processes(LOOK_FOR_PHASE1)
#    #print "Dictionary of processes stage 1: %s" % dict_processes_phase1
#
#    num_phase1_running = 0
#    for node in dict_processes_phase1.keys():
#        num_phase1_running += dict_processes_phase1[node]['frequency']
#    if num_phase1_running < 1:
#        logging.info("%s not found running on any node." % LOOK_FOR_PHASE1)
#        start_phase1(LOOK_FOR_PHASE1)
#    elif num_phase1_running > 1:
#        logging.info("Too many %s running.  Gonna kill them all and start fresh." % LOOK_FOR_PHASE1)
#        for each_node in dict_processes_phase1.keys():
#            if dict_processes_phase1['process_output']:
#                logging.info("Processes found on %s: %s" % (each_node, dict_processes_phase1[each_node]['process_output']))
#        kill_all(LOOK_FOR_PHASE1)
#        start_phase1(LOOK_FOR_PHASE1)
#
#    # Phase1 seconds, we should only have one running, verify have good heartbeat
#    last_heartbeat = get_last_heartbeat()
#    last_heartbeat_diff = get_heartbeat_diff(NUMBER_RUNNING_PHASE2)
#    ###########################################
#    ###########################################
#    #print "last_heartbeat: %s" % last_heartbeat
#    #print "last_heartbeat_diff: %s" % last_heartbeat_diff
#    #print "stale heartbeat:%s" % STALE_HEARTBEAT
#    #print "type stale heartbeat:%s" % type(STALE_HEARTBEAT)
#    ###########################################
#    ###########################################
#    if last_heartbeat_diff >= STALE_HEARTBEAT:
#        logging.info("%s has stale heartbeat:%s.  Killing all to start fresh." % (LOOK_FOR_PHASE1, last_heartbeat))
#        for each_node in dict_processes_phase1.keys():
#            if dict_processes_phase1['process_output']:
#                logging.info("Processes found on %s: %s" % (each_node, dict_processes_phase1[each_node]['process_output']))
#        kill_all(LOOK_FOR_PHASE1)
#        start_phase1(LOOK_FOR_PHASE1)
    

    # Phase2, we should have NUMBER_RUNNING_PHASE2 running on each node
    # Now will have to look in heartbeat file on each node for running processes PIDs

    dict_processes_phase2 = get_processes(LOOK_FOR_PHASE2)
    #print dict_processes_phase2
    # First lets see how many are running on each node and start more or kill some if needed
    for node in dict_processes_phase2.keys():
        _this_nodes_frequency = 0
        for each_ps_line in dict_processes_phase2[node]['process_output']:
            #print "this process output line is:  %s" % each_ps_line
            _this_nodes_frequency += 1
        dict_processes_phase2[node]['frequency'] = _this_nodes_frequency

        if dict_processes_phase2[node]['frequency'] < NUMBER_RUNNING_PHASE2:
            need = NUMBER_RUNNING_PHASE2 - dict_processes_phase2[node]['frequency']
            print "Need %s more %s on %s" % (need, LOOK_FOR_PHASE2, node)
            start_phase2(node, need)
        elif dict_processes_phase2[node]['frequency'] > NUMBER_RUNNING_PHASE2:
            have = dict_processes_phase2[node]['frequency'] - NUMBER_RUNNING_PHASE2
            print "Have too many %s running on %s.  Gonna kill all of them and start from scratch." % (LOOK_FOR_PHASE2, node)
            pids = get_pids(dict_processes_phase2[node]['process_output'])
            kill_em_all(node, pids)
            start_phase2(node, NUMBER_RUNNING_PHASE2)
        else:
            logging.info("Have the correct number of %s running on %s." % (LOOK_FOR_PHASE2, node))
            print "Have the correct number of %s running on %s." % (LOOK_FOR_PHASE2, node)
            
# ***********************************************************************************
#    # Then lets verify have current heartbeat for each process running on each node
#    # If not, kill processes that are stale and restart one for each killed
#    dict_processes_phase2 = get_processes(LOOK_FOR_PHASE2)
#    for each_node in dict_processes_phase2.keys():
#        print "Node:  %s" % each_node
#        for each_running_process in dict_processes_phase2[each_node]['process_output']:
#            print "ps output:     %s" % each_running_process
#            this_processes_pid = each_running_process.split(' ', 5)[-1]
#            print "PID is %s" % this_processes_pid
#            last_heartbeat_for_this_pid = ''
#            is_this_pid_stale = ''
#            if is_this_pid_stale is True:
#                #logging.info("Stale heartbeat for %s.  %s" % (each_running_process, last_heartbeat_for_this_pid))
#                #kill_this_pid(pid, node)
#            else:
#                #logging.info("Good heartbeat for %s.  %s" %  (each_running_process, last_heartbeat_for_this_pid))




if __name__ == '__main__':
    main()
