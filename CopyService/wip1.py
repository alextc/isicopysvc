import sys
import socket
import os
import time
import fcntl
import datetime
import json
import glob
import shutil
import Common.papi as PAPI
from multiprocessing import Process
import logging

# sys.path.append('/ifs/copy_svc/code/CopyService/aop')
# from logstartandexit import LogEntryAndExit

max_retry_count = 5
max_concurrent = 5
max_cpu_load = 50
max_stale_hb_time_in_seconds = 30
process_file = '/ifs/copy_svc/' + socket.gethostname() + '_process_list.dat'
process_file_persist = '/ifs/copy_svc/' + socket.gethostname() + '_process_list_persist.dat'
potential_work_target_string = "/ifs/zones/*/copy_svc/staging/*/*"
datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'
process_state = {"Init": "Init", "CopyOrig": "CopyOrig", "ReAcl": "ReAcl", "Move": "Move", "Cleanup": "Cleanup"}

logging.basicConfig(filename='wip.log', level=logging.DEBUG)
log = logging.getLogger("phase2_log")

class state_obj:
    def __init__(self, state, source_dir, process_dir):
        self.state = state
        self.source_dir = source_dir
        self.target_dir = get_target_from_source(source_dir)
        self.process_dir = process_dir

    def __str__(self):
        return "State:" + self.state + "\n" + \
               "Source:" + self.source_dir + "\n" + \
               "Target" + self.target_dir + "\n" + \
               "ProcessDir:" + self.process_dir + "\n"

def is_max_process_count_reached():
    logging.debug("Entered is_max_process_count_reached")
    assert os.path.exists(os.path.dirname(process_file)), "Process file directory not found"

    # First call scenario - no file yet
    if not os.path.isfile(process_file):
        logging.debug("Returning False since process file does not exist")
        return False

    for i in range(max_retry_count):
        try:
            with open(process_file) as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                processes = process_info.readlines()
                assert len(processes) > 0, "Empty processes file in is_max_process reached"
                assert len(processes) < max_concurrent + 1, "Exceeded max_concurrent processes value"
                return len(processes) < max_concurrent
        except IOError:
            logging.debug(sys.exc_info()[0])
            time.sleep(1)

    # TODO: check if the value of i is retained here, should the function ever get here
    assert i < max_retry_count, "Unable to open process file in max_process_running"

def get_current_process_count():
    logging.debug("Entered get_current_process_count")
    assert os.path.exists(os.path.dirname(process_file)), "Process file directory not found"

    # First call scenario - no file yet
    if not os.path.isfile(process_file):
        logging.debug("Returning 0 since process file does not exist")
        return 0

    with open(process_file) as process_info:
        fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        processes = process_info.readlines()
        if not processes:
            return 0
        else:
            return len(processes)

def increment_process_running_count():
    logging.debug("Entered increment_process_running_count")
    assert os.path.exists(os.path.dirname(process_file)), "Process file directory not found"

    for i in range(max_retry_count):
        try:
            with open(process_file, 'a+') as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                process_info.writelines(str(os.getpid()) + "\n")
            logging.debug("Incremented process count")
            return
        except IOError:
            logging.debug(sys.exc_info()[0])
            time.sleep(1)

    assert i < (max_retry_count), "Unable to update process count in add_process_running"

def decrement_process_count():
    assert os.path.exists(process_file), "Attempt to open a non-existing process file in decrement_process_count"
    for i in range(max_retry_count):
        try:
            with open(process_file, 'r') as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lines = process_info.readlines()
                assert len(lines) > 0, "Was asked to remove a process from an empty file"
                lines.pop()
                with open(process_file, 'w') as process_info:
                    fcntl.flock(process_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    process_info.writelines(lines)
        except:
            logging.debug(sys.exc_info()[0])
            time.sleep(1)

    if i == (max_retry_count - 1):
        logging.debug("Unable to update process count in add_process_running")
        raise ValueError("Unable to open process file")

def can_do_work():
    logging.debug("Entered can_do_work")
    return is_max_process_count_reached()

def get_work_available():
    stranded_work = get_stranded_work()
    if stranded_work:
        return stranded_work()

    return get_new_work

def get_new_work():
    potential_work_targets = glob.glob(potential_work_target_string)
    potential_work_targets.sort()
    if potential_work_targets:
        for each_potential_work_target in potential_work_targets:
            if not each_potential_work_target.endswith("_in_process"):
                ownership_state = take_ownership(each_potential_work_target, False)
                if ownership_state:
                    return ownership_state

def take_ownership(potential_work_target, ignore_prev_owner):
    expected_target_staging_dir = potential_work_target + "_in_process"

    if not ignore_prev_owner and os.path.exists(expected_target_staging_dir):
        # Since path exists and the caller wants to respect prior ownership -> Exit
        return
    elif not os.path.exists(expected_target_staging_dir):
        os.mkdir(expected_target_staging_dir)
        write_ownership_markers(potential_work_target)
    else:
        write_ownership_markers(potential_work_target)

    state = state_obj(state="Init", source_dir=potential_work_target, process_dir=expected_target_staging_dir)
    return state

def write_ownership_markers(potential_work_target):
    expected_target_staging_dir = potential_work_target + "_in_process"
    expected_hb_file = expected_target_staging_dir + "/hb.dat"
    expected_source_file = expected_target_staging_dir + "/source.dat"
    expected_owner_file = expected_target_staging_dir + "/owner_data.dat"

    for i in range(max_retry_count):
        try:
            with open(expected_owner_file, 'w+') as owner_file:
                fcntl.flock(expected_owner_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                owner_file.writelines(socket.gethostname() + ":" + str(os.getpid()))
            with open(expected_hb_file, 'w+') as hb_file:
                fcntl.flock(hb_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                hb_file.writelines(datetime.datetime.utcnow().strftime(datetime_format_string))
            with open(expected_source_file, 'w+') as source_file:
                fcntl.flock(source_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                source_file.writelines(potential_work_target)
        except:
            logging.debug(sys.exc_info()[0])
            time.sleep(1)

    if i == (max_retry_count - 1):
        raise ValueError("Unable to write ownership markers")

def get_target_from_source(source_dir):
    my_ret = source_dir.replace("staging", "from")
    return my_ret

def get_stranded_work():
    potential_strand_targets = glob.glob(potential_work_target_string)
    potential_strand_targets.sort()
    if potential_strand_targets:
        for each_potential_strand_target in potential_strand_targets:
            if each_potential_strand_target.endswith("_in_process"):
                if is_heartbeat_stale(each_potential_strand_target):
                    my_ret = take_ownership(get_original_source(each_potential_strand_target), True)
                    if my_ret:
                        my_ret.cur_state = get_state(each_potential_strand_target)
                        return my_ret

def get_original_source(ownership_path):
    original_source = ownership_path + "/source.dat"
    if os.path.exists(original_source):
        with open(original_source) as orig_source:
            my_ret = orig_source.readline().strip()
            return my_ret

    raise ValueError("Unable to get orignal source path")

def is_heartbeat_stale(ownership_path):
    hb_file = ownership_path + "/hb.dat"
    if not os.path.exists(hb_file):
        raise ValueError("Request to check heart beat on a non-existing hb.dat")

    for i in range(max_retry_count):
        try:
            with open(hb_file) as last_heartbeat:
                fcntl.flock(last_heartbeat.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                last_hb_time = last_heartbeat.readline().strip()
                if last_hb_time:
                    last_hb_datetime = datetime.datetime.strptime(last_hb_time, datetime_format_string)
                    return (datetime.datetime.utcnow() - last_hb_datetime).seconds > max_stale_hb_time_in_seconds

            time.sleep(1)
        except:
            logging.debug(sys.exc_info()[0])

    if i == (max_retry_count - 1):
        raise ValueError("Unable to check heartbeat")

def get_state(ownership_path):
    if not os.path.exists(ownership_path):
        raise ValueError("Request to check state on a state.dat failed")

    state_file_name = ownership_path + "/state.dat"
    with open(state_file_name) as state_file:
        my_ret = state_file.readline()
        if not my_ret:
            raise ValueError("No state found")
        return my_ret

def launch_script(script_path):
    os.system("python '" + script_path + "'")

def spawn_new_worker(should_wait):
    process_obj = Process(target=launch_script, args=(os.path.realpath(__file__),))
    process_obj.start()
    if should_wait:
        logging.debug("spawning new instance of script and waiting")
        process_obj.join()
    else:
        logging.debug("spawning new instance of script and not waiting")

def perform_heartbeat(state):
    with open(state.hb_file, 'w+') as hb_file:
        fcntl.flock(hb_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        cur_hb_time_str = datetime.datetime.utcnow().strftime(datetime_format_string)
        logging.de.log_debug("Current HB time: '" + cur_hb_time_str + "'")
        hb_file.writelines(cur_hb_time_str)

def needs_copy(state):
    perform_heartbeat(state)
    if os.path.exists(state.target_dir):
        return True

def save_state(state):
    perform_heartbeat(state)
    with open(state.state_file, 'w+') as state_file:
        state_file.writelines(state.cur_state)

def async_copy(source_dir, target_dir):
    for src_dir, dirs, files in os.walk(source_dir):
        dst_dir = src_dir.replace(source_dir, target_dir)
        for dir_ in dirs:
            dst_dir_ = os.path.join(dst_dir, dir_)
            if not os.path.exists(dst_dir_):
                os.mkdir(dst_dir_)

        for file_ in files:
            src_file = os.path.join(src_dir, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                src_file_stat = os.stat(src_file)
                dst_file_stat = os.stat(dst_file)
                if src_file_stat.st_mtime > dst_file_stat.st_mtime:
                    os.remove(dst_file)
                    shutil.copy(src_file, dst_dir)
            else:
                shutil.copy(src_file, dst_dir)

def perform_fast_copy(source_dir, target_dir):
    my_ret = Process(target=async_copy, args=(source_dir, target_dir))
    my_ret.start()

def process_finished(process_obj):
    if process_obj.is_alive():
        my_ret = False
    logging.debug("EXIT process_finished: '" + str(my_ret) + "'")
    return my_ret

def check_process_result(process_obj):
    if process_obj:
        my_ret = (process_obj.exitcode == 0)
    logging.debug("EXIT check_process_result: '" + str(my_ret) + "'")
    return my_ret

def copy_original_to_staging(state):
    copy_process_obj = perform_fast_copy(state.target_dir, state.source_dir)

    # TODO: does the fact of completing a copy terminates the process; Will this run forever?
    while (True):
        perform_heartbeat(state)
        if process_finished(copy_process_obj):
            break
        time.sleep(1)

    my_ret = check_process_result(copy_process_obj)
    return my_ret

def get_source_acls(source_dir):
    acl_string = PAPI.grab_aclfromobj(source_dir)
    if acl_string:
        my_ret = json.loads(acl_string)
        return my_ret

    raise ValueError("Unable to get ACL template")

def set_dest_acls(source_dir, acl):
    my_ret = PAPI.set_aclonobj(source_dir, acl)
    logging.debug("EXIT set_dest_acls: '" + str(my_ret) + "'")
    return my_ret

def async_reacl(source_dir, dest_dir):
    logging.debug(source_dir)
    logging.debug(dest_dir)

    acls = get_source_acls(source_dir)
    result = set_dest_acls(dest_dir, acls)

    # TODO: Is this right?
    if result:
        error_hit = True

    for root, dirs, files in os.walk(dest_dir, topdown=False):
        for name in files:
            result = set_dest_acls(os.path.join(root, name), acls)
            if result:
                error_hit = True
        for name in dirs:
            result = set_dest_acls(os.path.join(root, name), acls)
            if result:
                error_hit = True

    if not error_hit:
        my_ret = True

    return my_ret

def perform_fast_reacl(source_dir, dest_dir):
    logging.debug(source_dir)
    logging.debug(dest_dir)
    my_ret = Process(target=async_reacl, args=(source_dir, dest_dir))
    my_ret.start()
    logging.debug("EXIT perform_fast_reacl: '" + str(my_ret) + "'")
    return my_ret

def reacl_staging(state):
    reacl_process_obj = perform_fast_reacl(os.path.split(state.target_dir)[0], state.source_dir)

    while True:
        perform_heartbeat(state)
        if process_finished(reacl_process_obj):
            break
        time.sleep(1)

    my_ret = check_process_result(reacl_process_obj)
    return my_ret

def async_move(state):
    if os.path.exists(state.target_dir):
        shutil.move(state.target_dir, state.process_dir + "/old_source_dir")

    shutil.move(state.source_dir, state.target_dir)
    my_ret = True
    return my_ret

def perform_fast_move(state):
    my_ret = Process(target=async_move, args=(state,))
    my_ret.start()
    return my_ret

def move_staging(state):
    move_process_obj = perform_fast_move(state)
    while (True):
        perform_heartbeat(state)
        if process_finished(move_process_obj):
            break
        time.sleep(1)

    my_ret = check_process_result(move_process_obj)
    return my_ret

def async_rmdir(directory):
    shutil.rmtree(directory)

def perform_fast_rmdir(source_dir):
    my_ret = Process(target=async_rmdir, args=(source_dir,))
    my_ret.start()
    return my_ret

def cleanup_staging(state):
    cleanup_process_obj = perform_fast_rmdir(state.process_dir)

    # TODO: Does the process exist after completing the func call - this may run forever
    while (True):
        if process_finished(cleanup_process_obj):
            break
        time.sleep(1)

    my_ret = check_process_result(cleanup_process_obj)
    return my_ret

def process_work(state):
    while True:
        if state.cur_state == "Init":
            if needs_copy(state):
                state.cur_state = "CopyOrig"
                save_state(state)
                if copy_original_to_staging(state):
                    state.cur_state = "ReAcl"
                    save_state(state)
            else:
                state.cur_state = "ReAcl"
                save_state(state)

        if state.cur_state == "CopyOrig":
            if copy_original_to_staging(state):
                state.cur_state = "ReAcl"
                save_state(state)

        if state.cur_state == "ReAcl":
            if reacl_staging(state):
                state.cur_state = "Move"
                save_state(state)

        if state.cur_state == "Move":
            if move_staging(state):
                state.cur_state = "Cleanup"
                save_state(state)

        if state.cur_state == "Cleanup":
            cleanup_staging(state)

        # All stages completed exit
        return

if __name__ == '__main__':
    try:
        if can_do_work():
            increment_process_running_count()
            my_work = get_work_available()
            if my_work:
                perform_heartbeat(my_work)
                spawn_new_worker(False)
                process_work(my_work)
    except KeyboardInterrupt:
        logging.debug("ctrl-c detected")
    except Exception as e:
        logging.debug(e)
        raise
    finally:
        if get_current_process_count() > 0:
            decrement_process_count()
