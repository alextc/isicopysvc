import os
import sys
import ConfigParser
import datetime
import logging
import subprocess

CONFIG_FILE = "/ifs/copy_svc/config"

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
    global HEARTBEAT_LOG_PHASE1, HEARTBEAT_LOG_PHASE2

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

    HEARTBEAT_LOG_PHASE1 = conf.get('Check_running_one_node', 'HB_file_1')
    HEARTBEAT_LOG_PHASE2 = conf.get('Check_running_each_node', 'HB_file_2')

def get_date():
    """
    Get date and time in a specific format
    :return: Comma separated date and time
    """
    return datetime.datetime.now().strftime("%Y, %m, %d, %H, %M, %S")

def get_last_heartbeat_phase1():
    """
    Read last line of the log file
    :return: timestamp (2015, 06, 23, 06, 29, 43)
    """
    with open(HEARTBEAT_LOG_PHASE1, 'r') as f:
        this_heartbeat = f.readlines()[-1].rstrip()
        this_edited_heartbeat = this_heartbeat.split(',')[0].strip('[')
        a_edited_this_heartbeat = this_edited_heartbeat.replace(' ', ', ')
        b_edited_this_heartbeat = a_edited_this_heartbeat.replace('-', ', ')
        final_edited_this_heartbeat = b_edited_this_heartbeat.replace(':', ', ')
        return final_edited_this_heartbeat

def get_heartbeat_diff_phase1():
    """
    Difference in seconds
    :return: integer (seconds)
    """
    current = datetime.datetime.strptime(get_date(), '%Y, %m, %d, %H, %M, %S')
    last_record = datetime.datetime.strptime(get_last_heartbeat_phase1(), '%Y, %m, %d, %H, %M, %S')
    return (current - last_record).seconds

def get_processes(process_name):
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
                '|', 'grep', process_name,
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

    return my_ret

def kill_all(process_to_kill):
    """
    Kill all the running processes
    :return: None
    """
    cmd = "isi_for_array ps auxwww|grep {0} |grep -v grep|".format(process_to_kill) + r"awk '{print$3}'"
    process = subprocess.Popen(['/bin/bash', '-c', cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    communicate = filter(None, process.communicate()[0].split('\n'))
    null = open(os.devnull, 'w')

    for i in communicate:
        logging.info("Killing {0} now".format(i))
        subprocess.Popen(['/bin/bash', '-c', '/usr/bin/isi_for_array /bin/kill {0} '.format(i)], stderr=null,
              stdout=null).communicate()

def kill_this_pid(pid, node):
    """ Given pid and node, kill it"""
    pass

def kill_em_all(node, pids):
    """ Kill all pids on node"""
    nodes_lnn = node.rsplit('-', 1)[-1]
    for each_pid in pids:
        #logging...
        command = "isi_for_array -n %s kill %s" % (nodes_lnn, each_pid)
        subprocess.Popen(command.split())
    return

def start_phase1(phase1_process):
    """
    Start/Restart new process
    :return: None
    """
    logging.info("Starting {0} now".format(phase1_process))
    subprocess.Popen(['/bin/bash', '-c', '/usr/bin/isi_ropc /usr/bin/python {0} & disown'.format(phase1_process)])
    return

def start_phase2(node, need):
    nodes_lnn = node.rsplit('-', 1)[-1]
    logging.info("Need %s more %s on %s" % (need, LOOK_FOR_PHASE2, node))
    for i in range(1, need+1):
        logging.info("Starting %s now on node %s" % (LOOK_FOR_PHASE2, node))
        command = "isi_for_array -n %s /usr/bin/python %s & disown" % (nodes_lnn, LOOK_FOR_PHASE2)
        subprocess.Popen(command.split())
    return

def get_pids(process_output):
    """Given list of lines of process output, return pids"""
    _list_of_pids = []
    for each_running_process in process_output:
        this_processes_pid = each_running_process.split()[2]
        _list_of_pids.append(this_processes_pid)
    return _list_of_pids

def get_heartbeat_for_pid_on_node(node, pid):
    #Host:b7201-1
    #PID:82070
    #ACL Template Dir:/ifs/zones/ad2/copy_svc/from/ad1
    #Heartbeat:2015-07-16 12:04:17
    #[2015-07-16 12:04:19,011 82070] Setting state to Move
    #[2015-07-16 12:04:19,036 82070] About to move /ifs/zones/ad1/copy_svc/staging/ad2/258384 to /ifs/zones/ad2/copy_svc/from/ad1/258384
    pass
            
def check_stale_phase2(node, pid):
    """Given a phase2 PID and node PID on, return True if stale""" 
    last_seen = get_heartbeat_for_pid_on_node(node, pid)
    if last_seen >= STALE_HEARTBEAT:
        state = True
    else:
        state = False
    return (state, last_seen)

def main():
    set_config()
    #HEARTBEAT_LOG_PHASE1 = '/ifs/copy_svc/Phase1Worker.log.STALE'

    # Phase1:  We only want one instance running on one node
    # ------------------------------------------------------
    # First determine number processes running cluster-wide
    dict_processes_phase1 = get_processes(LOOK_FOR_PHASE1)
    num_phase1_running = 0
    for node in dict_processes_phase1.keys():
        num_phase1_running += dict_processes_phase1[node]['frequency']
    if num_phase1_running < 1:
        logging.info("%s not found running on any node." % LOOK_FOR_PHASE1)
        start_phase1(LOOK_FOR_PHASE1)
    elif num_phase1_running > 1:
        logging.info("Too many %s running.  Gonna kill them all and start fresh." % LOOK_FOR_PHASE1)
        for each_node in dict_processes_phase1.keys():
            if dict_processes_phase1['process_output']:
                logging.info("Processes found on %s: %s" % (each_node, dict_processes_phase1[each_node]['process_output']))
        kill_all(LOOK_FOR_PHASE1)
        start_phase1(LOOK_FOR_PHASE1)
    else:
        logging.info("Have the correct number of %s running" % LOOK_FOR_PHASE1)
        for each_node in dict_processes_phase1.keys():
            if dict_processes_phase1[each_node]['process_output']:
                logging.info("Processes found %s" % dict_processes_phase1[each_node]['process_output'])

    # Second verify have good heartbeat
    last_heartbeat = get_last_heartbeat_phase1()
    last_heartbeat_diff = get_heartbeat_diff_phase1()
    if last_heartbeat_diff >= STALE_HEARTBEAT:
        logging.info("%s has stale heartbeat:%s.  Killing all to start fresh." % (LOOK_FOR_PHASE1, last_heartbeat))
        kill_all(LOOK_FOR_PHASE1)
        start_phase1(LOOK_FOR_PHASE1)
    for each_node in dict_processes_phase1.keys():
        if dict_processes_phase1[each_node]['process_output']:
            logging.info("Heartbeat found %s" % last_heartbeat)

    # Phase2:  We should have NUMBER_RUNNING_PHASE2 running on each node
    # ------------------------------------------------------------------
    # First see how many are running on each node, and react
    dict_processes_phase2 = get_processes(LOOK_FOR_PHASE2)
    for node in dict_processes_phase2.keys():
        _this_nodes_frequency = 0
        for each_ps_line in dict_processes_phase2[node]['process_output']:
            _this_nodes_frequency += 1
        dict_processes_phase2[node]['frequency'] = _this_nodes_frequency

        if dict_processes_phase2[node]['frequency'] < NUMBER_RUNNING_PHASE2:
            need = NUMBER_RUNNING_PHASE2 - dict_processes_phase2[node]['frequency']
            start_phase2(node, need)
        elif dict_processes_phase2[node]['frequency'] > NUMBER_RUNNING_PHASE2:
            have = dict_processes_phase2[node]['frequency'] - NUMBER_RUNNING_PHASE2
            pids = get_pids(dict_processes_phase2[node]['process_output'])
            # Why kill them all ???
            kill_em_all(node, pids)
            start_phase2(node, NUMBER_RUNNING_PHASE2)
        else:
            logging.info("Have the correct number of %s running on %s." % (LOOK_FOR_PHASE2, node))
            
    # Second verify have current heartbeat for each process running
    dict_processes_phase2 = get_processes(LOOK_FOR_PHASE2)
    for each_node in dict_processes_phase2.keys():
        for each_running_process in dict_processes_phase2[each_node]['process_output']:
            this_processes_pid = each_running_process.split()[2]
            is_this_pid_stale, last_heartbeat_for_this_pid = check_stale_phase2(each_node, this_processes_pid)
            if is_this_pid_stale:
                #logging.info("Stale heartbeat.  Killing %s on node %s.  %s" %  (each_running_process, node, last_heartbeat_for_this_pid))
                #kill_this_pid(pid, node)
                pass
            else:
                #logging.info("Good heartbeat for %s on node %s.  %s" %  (each_running_process, node, last_heartbeat_for_this_pid))
                pass



if __name__ == '__main__':
    main()
