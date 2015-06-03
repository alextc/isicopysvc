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
from work.processpool import ProcessPool
from work.workscheduler import WorkScheduler
from model.workstate import WorkState

max_retry_count = 5
max_concurrent = 5
max_cpu_load = 50
max_stale_hb_time_in_seconds = 30
potential_work_target_string = "/ifs/zones/*/copy_svc/staging/*/*"
datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'
process_state = {"Init": "Init", "CopyOrig": "CopyOrig", "ReAcl": "ReAcl", "Move": "Move", "Cleanup": "Cleanup"}
logging.basicConfig(filename='/ifs/copy_svc/wip.log',level=logging.DEBUG)

def get_work_available():
    logging.debug("Entered get_work_available")
    work_scheduler = WorkScheduler()
    stranded_work = work_scheduler.get_stranded_work()
    if stranded_work:
        return stranded_work()

    return get_new_work

def get_new_work():
    work_scheduler = WorkScheduler()
    potential_work_targets = glob.glob(potential_work_target_string)
    potential_work_targets.sort()
    if potential_work_targets:
        for each_potential_work_target in potential_work_targets:
            if not each_potential_work_target.endswith("_in_process"):
                ownership_state = work_scheduler.take_ownership(each_potential_work_target, False)
                if ownership_state:
                    return ownership_state

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
    process_file = "/ifs/copy_svc/isilab-1_process_list1.dat"
    process_pool = ProcessPool()
    try:
        if not process_pool.is_max_process_count_reached():
            process_pool.increment_process_running_count()
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
        logging.debug("Entering Finally in Main")
        if process_pool.get_current_process_count() > 0:
            process_pool.decrement_process_count()
