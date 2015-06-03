import ConfigParser
import os
import time
import fcntl
import datetime
import socket
import json
import glob
import shutil
import Common.papi as PAPI
from multiprocessing import Process
import logging
from work.processpool import ProcessPool
from work.workscheduler import WorkScheduler
from model.workstate import WorkState
from sql.heartbeatdb import HeartBeatDb

"""
# Getting Config.Parser.NoSectoinError: No section: Varaiables
settings = ConfigParser.ConfigParser()
settings.read('/ifs/copy_svc/config')

max_retry_count = settings.get('Variables', 'Retry')
max_concurrent = settings.get('Variables', 'Concurrent')
max_cpu_load = settings.get('Variables', 'CPU')
max_stale_hb_time_in_seconds = settings.get('Variables', 'Stale_HB')

potential_work_target_string = settings.get('Paths', 'Work_target')
datetime_format_string = settings.get('Variables', 'Formatter')

"""
potential_work_target_string = "/ifs/zones/*/copy_svc/staging/*/*/"
datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'
process_state = {"Init": "Init", "CopyOrig": "CopyOrig", "ReAcl": "ReAcl", "Move": "Move", "Cleanup": "Cleanup"}
logging.basicConfig(filename='/ifs/copy_svc/wip.log',level=logging.DEBUG)

def get_work_available():
    logging.debug("Entered get_work_available")
    work_scheduler = WorkScheduler()
    stranded_work = work_scheduler.get_stranded_work()
    if stranded_work:
        return stranded_work()

    return work_scheduler.get_new_work()


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
    # TODO: Optimize - probably better to pass db connection object instead of newing this up every second
    heart_beat_db_path = "/ifs/copy_svc/" + socket.gethostname() + "_heartbeat.db"
    heart_beat_db = HeartBeatDb(heart_beat_db_path)
    heart_beat_db.write_heart_beat()
    logging.debug("Heartbeat DB dump: {0}".format(heart_beat_db.dump()))

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
    process_pool = ProcessPool()
    if not process_pool.is_max_process_count_reached():
        my_work = get_work_available()
        if my_work:
            perform_heartbeat(my_work)
            # Making this work with single thread for now
            # spawn_new_worker(False)
            process_work(my_work)
