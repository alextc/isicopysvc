__author__ = 'Kailash Joshi'
import os
import ConfigParser
import datetime
import logging
from subprocess import Popen, PIPE
import sys
import re

CONFIG_FILE = "/ifs/copy_svc/config"
LOOK_FOR = None
PARENT_LOG_PATH = None
LOG_FILENAME = None
PATH_TO_LOG = None
STALE_HEARTBEAT = None
ROTATE_FREQUENCY = None
HEARTBEAT_LOG = '/ifs/copy_svc/Phase1Worker.log'

# Sample heartbeats:
#Phase1WorkScheduler.log
#[2015-06-25 14:11:00,493 89156] No Phase1 Source Dirs found
#
#Phase1Worker.log
#[2015-06-25 14:10:54,825 89156] No still items were detected exiting run


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
    global LOOK_FOR, PARENT_LOG_PATH, LOG_FILENAME, PATH_TO_LOG, STALE_HEARTBEAT, ROTATE_FREQUENCY
    LOOK_FOR = conf.get('Check_running_one_node', 'Process')
    PARENT_LOG_PATH = conf.get('Check_running_one_node', 'LogPath')
    LOG_FILENAME = conf.get('Check_running_one_node', 'LogFile')
    PATH_TO_LOG = os.path.join(PARENT_LOG_PATH, LOG_FILENAME)
    logging.basicConfig(filename=PATH_TO_LOG, level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)d %(message)s',
                        datefmt="%Y,%m,%d,%H,%M,%S")
    STALE_HEARTBEAT = conf.get('Check_running_one_node', 'Stale')
    ROTATE_FREQUENCY = conf.get('Check_running_one_node', 'Rotate')


def get_date():
    """
    Get UTC date and time in a specific format
    :return: Comma separated date and time
    """
    #return datetime.datetime.utcnow().strftime("%Y, %m, %d, %H, %M, %S")
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def get_last_heartbeat():
    """
    Read last line of the log file
    :return: timestamp (2015, 06, 23, 06, 29, 43)
    """
    with open(HEARTBEAT_LOG, 'r') as f:
        return f.readlines()[-1].rstrip()


def get_heartbeat_diff():
    """
    Difference in seconds
    :return: integer (seconds)
    """
    current = datetime.datetime.strptime(get_date(), '%Y, %m, %d, %H, %M, %S')
    last_record = datetime.datetime.strptime(get_last_heartbeat(), '%Y, %m, %d, %H, %M, %S')
    return (current - last_record).seconds


def get_running():
    """
    List all the  processes running.
    :return: list of processes
    """
    process = Popen(
        ['isi_for_array', '-X', 'ps', 'auxwww', '|grep', LOOK_FOR, '|grep', '-v', 'grep|grep', '-v',
         '"sh -c /usr/bin/python {0}"'.format(LOOK_FOR), '|grep', '-v', 'isi_rdo|grep', '-v', 'isi_for_array|grep',
         '-v',
         'ropc'], stdout=PIPE, stderr=PIPE)
    communicate = process.communicate()
    return filter(None, communicate[0].split('\n'))


def kill_all():
    """
    Kill all the running processes
    :return: None
    """
    cmd = "isi_for_array ps auxwww|grep {0} |grep -v grep|".format(LOOK_FOR) + r"awk '{print$3}'"
    process = Popen(['/bin/bash', '-c', cmd], stdout=PIPE, stderr=PIPE)
    communicate = filter(None, process.communicate()[0].split('\n'))
    null = open(os.devnull, 'w')

    for i in communicate:
        Popen(['/bin/bash', '-c', '/usr/bin/isi_for_array /bin/kill {0} '.format(i)], stderr=null,
              stdout=null).communicate()


def start_process():
    """
    Start/Restart new process
    :return: None
    """
    logging.info("Starting {0} now".format(LOOK_FOR))
    Popen(['/bin/bash', '-c', '/usr/bin/isi_ropc /usr/bin/python {0} & disown'.format(LOOK_FOR)])
    return


def get_frequency(process_list):
    """
    Get number of processes running
    :param process_list: list of processes
    :return: Number of processes
    """
    first_item = process_list[0]
    if 'exited with status 1' in first_item:
        return 0
    else:
        return len(process_list)


def test_ok(running, frequency, last_heartbeat, last_heartbeat_diff):
    """
    Check if the processing is running properly
    :param running:
    :param frequency:
    :param last_heartbeat:
    :param last_heartbeat_diff:
    :return: None
    """
    if frequency == 1 and last_heartbeat_diff < int(STALE_HEARTBEAT):
        logging.info('Found {0} with current heartbeat {1}'.format(LOOK_FOR, last_heartbeat))
        logging.info(running[0])
        print 'Test Passed'
        sys.exit(0)


def main():
    set_config()
    running = get_running()
    frequency = get_frequency(running)
    last_heartbeat = get_last_heartbeat()
    last_heartbeat_diff = get_heartbeat_diff()
    test_ok(running, frequency, last_heartbeat, last_heartbeat_diff)
    if frequency == 0:
        logging.info('{0} not found anywhere.  Starting now...'.format(LOOK_FOR))
    elif frequency > 1:
        logging.info("Too many {0} running.".format(LOOK_FOR))
        logging.info("PS output ---> {0} < ---".format(running))
        logging.info("Killing all processes across the cluster.")
        kill_all()
    elif last_heartbeat_diff > int(STALE_HEARTBEAT):
        logging.info("{0} running with stale heartbeat {1}".format(LOOK_FOR, last_heartbeat))
        logging.info("PS output ---> {0} < ---".format(running))
        logging.info("Killing {0} on {1}".format(LOOK_FOR, running))
        null = open(os.devnull, 'w')
        for i in running:
            _all_data = i.split()
            _node_id = re.findall('\d+', _all_data[0])[0]
            _pid = _all_data[2]
            Popen(['/bin/bash', '-c', '/usr/bin/isi_for_array -n {0} /bin/kill {1} '.format(_node_id, _pid)],
                  stderr=null, stdout=null).communicate()

    else:
        logging.info("Something is unexpected.")
        logging.info("PS output ---> {0} < ---".format(running))
        logging.info("Last heartbeat:  {0}".format(last_heartbeat))
    start_process()
    restart = get_running()
    logging.info('PS output ---> {0} <---'.format(restart[0]))


if __name__ == '__main__':
    main()
