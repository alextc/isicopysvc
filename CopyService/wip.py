''' Long running script to look for WIP
Looks for dirs in staging
if see work, is it in progress
  if yes, move on
  if no, claim work
if able to claim work
is move?
if yes, start permissions repair job, update db with JID
if no, start thread/script/x for copy
'''
import sys
import socket
import os
import fileinput
import time
import fcntl
import datetime
import json
import glob
import shutil
import Common.Logging as Logger

max_proceses = 5
max_cpu_load = 50
max_stale_hb_time_in_seconds = 30
process_file = '/ifs/copy_svc/' + socket.gethostname() + '_process_list.dat'
potential_work_target_string = "/ifs/zones/*/copy_svc/staging/*/*"
datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'
process_state = {"Init":"Init", "CopyOrig":"CopyOrig", "ReAcl":"ReAcl", "Move":"Move", "Cleanup":"Cleanup"}

class state_obj:
    cur_state = "Init"
    source_dir = ""
    target_dir = ""
    process_dir = ""
    hb_file = ""
    state_file = ""

def max_proceses_running():
    Logger.log_debug("ENTER max_proceses_running")
    my_ret = False
    if os.path.isfile(process_file):
        retry_count = 0
        while(True):
            if retry_count > 10:
                break
            try:
                with open(process_file) as process_info:
                    processes = process_info.readlines()
                    if processes:
                        if processes.count < max_proceses:
                            my_ret = True
                break
            except Exception as e:
                Logger.log_exception(e)
                time.sleep(1)
                retry_count += 1
    Logger.log_debug("EXIT max_proceses_running'" + str(my_ret) + "'")
    return my_ret

def add_process_running():
    Logger.log_debug("ENTER add_process_running")
    retry_count = 0
    my_ret = False
    while(True):
        if retry_count > 10:
            break
        try:
            with open(process_file, 'w+') as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX)
                process_info.writelines(str(os.getpid()))
                my_ret = True
            break
        except:
           time.sleep(1)
           retry_count += 1 
    Logger.log_debug("EXIT add_process_running'" + str(my_ret) + "'")
    return my_ret

def remove_process_running():
    Logger.log_debug("ENTER remove_process_running")
    retry_count = 0
    my_ret = False
    while(True):
        if retry_count > 300:
            break
        try:
            lines = []
            with open(process_file, 'r') as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX)
                lines = process_info.readlines()
            if lines:
                with open(process_file,'w') as process_info:
                    fcntl.flock(process_file, fcntl.LOCK_EX)
                    first_line = True
                    for each_line in lines:
                        if not first_line:
                            process_info.writelines(each_line)
                            first_line = False
            my_ret = True
            break
        except:
           time.sleep(1)
           retry_count += 1 
    Logger.log_debug("EXIT remove_process_running'" + str(my_ret) + "'")

def current_cpu_utilization():
    Logger.log_debug("ENTER current_cpu_utilization")
    my_ret = 2
    # grab this from os.popen('uptime')
    # TODO:
    Logger.log_debug("EXIT current_cpu_utilization'" + str(my_ret) + "'")
    return my_ret

def can_do_work():
    Logger.log_debug("ENTER can_do_work")
    my_ret = not max_proceses_running()
    if my_ret:
        my_ret = current_cpu_utilization() < max_cpu_load
    Logger.log_debug("EXIT can_do_work: '" + str(my_ret) + "'")
    return my_ret

def get_work_available():
    Logger.log_debug("ENTER get_work_available")
    my_ret = get_stranded_work()
    if not my_ret:
        my_ret = get_new_work()

    Logger.log_debug("EXIT get_work_available: '" + str(my_ret) + "'")
    return my_ret

def get_new_work():
    Logger.log_debug("ENTER get_new_work")
    my_ret = ""
    potential_work_targets = glob.glob(potential_work_target_string)
    if potential_work_targets:
        for each_potential_work_target in potential_work_targets:
            if not each_potential_work_target.endswith("_in_process"):
                my_ret = take_ownership(each_potential_work_target, False)
                break
    Logger.log_debug("EXIT get_new_work: '" + str(my_ret) + "'")
    return my_ret

def take_ownership(potential_work_target, ignore_prev_owner):
    Logger.log_debug("ENTER take_ownership")
    my_ret = None
    retry_count = 0
    expected_path = get_ownership_path(potential_work_target)
    expected_hb_file = expected_path + "/hb.dat"
    expected_state_file = expected_path + "/state.dat"
    expected_source_file = expected_path + "/source.dat"
    expected_owner_file = expected_path + "/owner_data.dat"
    while(True):
        try:
            potential_takes = glob.glob(potential_work_target + "_in_process")
            if not potential_takes or ignore_prev_owner :
               if not os.path.exists(expected_path):
                   os.mkdir(expected_path)
               with open(expected_owner_file, 'w+') as owner_file:
                   owner_file.writelines(socket.gethostname() + ":" + str(os.getpid()))
               with open(expected_hb_file,'w+') as hb_file:
                   hb_file.writelines(datetime.datetime.utcnow().strftime(datetime_format_string))
               with open(expected_state_file, 'w+') as state_file:
                   state_file.writelines("Init")
               with open(expected_source_file, 'w+') as source_file:
                    source_file.writelines(potential_work_target)
               my_ret = state_obj()
               my_ret.state = "Init"
               my_ret.source_dir = potential_work_target
               my_ret.target_dir = get_target_from_source(my_ret.source_dir)
               my_ret.process_dir = expected_path
               my_ret.hb_file = expected_hb_file
               my_ret.state_file = expected_state_file

            break
        except Exception as e:
            Logger.log_exception(e)
            if retry_count > 5:
                break
            else:
                time.sleep(1)
                retry_count += 1
                
    Logger.log_debug("EXIT take_ownership: '" + str(my_ret) + "'")
    return my_ret

def get_target_from_source(source_dir):
    Logger.log_debug("ENTER get_target_from_source")
    my_ret = source_dir.replace("staging","from")
    Logger.log_debug("EXIT get_target_from_source: '" + str(my_ret) + "'")
    return my_ret

def get_stranded_work():
    Logger.log_debug("ENTER get_stranded_work")
    my_ret = None
    potential_strand_targets = glob.glob(potential_work_target_string)
    if potential_strand_targets:
        for each_potential_strand_target in potential_strand_targets:
            if each_potential_strand_target.endswith("_in_process"):
                if stale_heartbeat(each_potential_strand_target):
                    my_ret = take_ownership(get_original_source(each_potential_strand_target), True)
                    if my_ret:
                        my_ret.state = get_state(each_potential_strand_target)
                        break
    Logger.log_debug("EXIT get_stranded_work: '" + str(my_ret) + "'")
    return my_ret

def get_original_source(ownership_path):
    Logger.log_debug("ENTER get_original_source")
    my_ret = ""
    original_source = ownership_path + "/source.dat"
    if os.path.exists(original_source):
        with open(original_source) as orig_source:
            my_ret = orig_source.readline().strip()
    Logger.log_debug("EXIT get_original_source: '" + str(my_ret) + "'")
    return my_ret

def stale_heartbeat(ownership_path):
    Logger.log_debug("ENTER stale_heartbeat")
    my_ret = True
    hbFile = ownership_path + "/hb.dat"
    with open(hbFile) as last_heartbeat:
        last_hb_time = last_heartbeat.readline().strip()
        if last_hb_time:
            last_hb_datetime = datetime.datetime.strptime(last_hb_time, datetime_format_string)
            if (datetime.datetime.utcnow() - last_hb_datetime).seconds < max_stale_hb_time_in_seconds:
                my_ret = False
    Logger.log_debug("EXIT stale_heartbeat: '" + str(my_ret) + "'")
    return my_ret

def get_state(ownership_path):
    Logger.log_debug("ENTER get_state")
    my_ret = ""
    state_file_name = ownership_path + "/state.dat"
    with open(state_file_name) as state_file:
        my_ret = state_file.readline()
    Logger.log_debug("EXIT get_state: '" + str(my_ret) + "'")
    return my_ret

def get_ownership_path(path):
    Logger.log_debug("ENTER get_ownership_path")
    my_ret = path + "_in_process"
    Logger.log_debug("EXIT get_ownership_path: '" + str(my_ret) + "'")
    return my_ret

def look_for_folders():
    # am i not resource constrainded
    # am i not performing max concurrent work
    # is work available
    pass
    return object, step

def is_copy(folder):
    pass

def is_dir_in_progress(folder):
    # folder exists
    pass

def claim_move_dir_job(folder):
    pass

def claim_copy_dir_job(folder):
    pass

def start_ACL_repair_job(folder):
    pass
    return job_id

def set_ACL_repair_job_id(folder, job_id):
    pass

def start_copy_job(folder):
    pass

def get_ACL_job_in_progress():
    # select where acl_jobid not null and is completed is false
    pass

def get_isi_job_status():
    pass
    # statuses is list with tuples each with job_id, Succeeded
    # isi job events list --job-id 170 | grep Succeeded
    return statuses

def spawn_new_worker(should_wait):
    Logger.log_debug("ENTER spawn_new_worker")
    if should_wait:
        #os.spawnl(os.P_WAIT, "python", "my script path")
        Logger.log_debug("spawning new instance of script and waiting")
    else:
        #os.spawnl(os.P_NOWAIT,"python", "my script path")
        Logger.log_debug("spawning new instance of script and not waiting")

    Logger.log_debug("EXIT spawn_new_worker")

def perform_heartbeat(state):
    Logger.log_debug("ENTER perform_heartbeat")
    with open(state.hb_file,'w+') as hb_file:
        fcntl.flock(hb_file.fileno(), fcntl.LOCK_EX)
        cur_hb_time_str = datetime.datetime.utcnow().strftime(datetime_format_string)
        log_debug("Current HB time: '" + cur_hb_time_str + "'")
        hb_file.writelines(cur_hb_time_str)
    Logger.log_debug("EXIT perform_heartbeat")


def needs_copy(state):
    Logger.log_debug("ENTER needs_copy")
    my_ret = False
    perform_heartbeat(state)
    if os.path.exists(state.target_dir):
        my_ret = True

    Logger.log_debug("EXIT needs_copy: '" + str(my_ret) + "'")
    return my_ret

def save_state(state):
    Logger.log_debug("ENTER save_state")
    perform_heartbeat(state)
    with open(state.state_file,'w+') as state_file:
        state_file.writelines(state.cur_state)
    Logger.log_debug("EXIT save_state")

def perform_fast_copy(source_dir, target_dir, recursive):
    Logger.log_debug("ENTER perform_fast_copy")
    my_ret = None

    # TODO figure out a spawned copy process to check
    Logger.log_debug("EXIT perform_fast_copy: '" + str(my_ret) + "'")
    return my_ret

def process_finished(process_obj):
    Logger.log_debug("ENTER process_finished")
    my_ret = True
    if process_obj:
       my_ret = process_obj.HasExited()
    Logger.log_debug("EXIT process_finished: '" + str(my_ret) + "'")
    return my_ret

def check_process_result(process_obj):
    Logger.log_debug("ENTER check_process_result")
    my_ret = False
    if process_obj:
        my_ret = (process_obj.ExitCode == 0)
    Logger.log_debug("EXIT check_process_result: '" + str(my_ret) + "'")
    return my_ret

def copy_original_to_staging(state):
    Logger.log_debug("ENTER copy_original_to_staging")
    my_ret = False
    copy_in_progress = False
    copy_process_obj = ""
    while(True):
        perfrom_heartbeat(state)
        if not copy_in_progress:
            copy_process_obj = perform_fast_copy(state.target_dir, state.source_dir, True)
            copy_in_progress = True
        else:
            if process_finished(copy_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(copy_process_obj)
    Logger.log_debug("EXIT copy_original_to_staging: '" + str(my_ret) + "'")
    return my_ret

def perform_fast_reacl(source_dir, dest_dir):
    Logger.log_debug("ENTER perform_fast_reacl")
    my_ret = None

    #TODO find out some super cool reacl thingy
    Logger.log_debug("EXIT perform_fast_reacl: '" + str(my_ret) + "'")
    return my_ret


def reacl_staging(state):
    Logger.log_debug("ENTER reacl_staging")
    my_ret = False
    reacl_in_progress = False
    reacl_process_obj = ""
    while(True):
        perfrom_heartbeat(state)
        if not reacl_in_progress:
            reacl_process_obj = perform_fast_reacl(os.path.join(state.target_dir,os.pardir), state.source_dir)
            reacl_in_progress = True
        else:
            if process_finished(reacl_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(reacl_process_obj)
    Logger.log_debug("EXIT reacl_staging: '" + str(my_ret) + "'")
    return my_ret

def move_staging(state):
    Logger.log_debug("ENTER move_staging")
    my_ret = False
    perfrom_heartbeat(state)
    if os.path.exists(state.target_dir):
        shutil.move(state.target_dir, state.process_dir + "/old_source_dir")

    shutil.move(state.source_dir, state.target_dir)
    my_ret = True
    Logger.log_debug("EXIT move_staging: '" + str(my_ret) + "'")
    return my_ret

def perform_fast_rmdir(source_dir):
    Logger.log_debug("ENTER perform_fast_rmdir")
    my_ret = False

    #TODO find a way to do fast cleanup
    Logger.log_debug("EXIT perform_fast_rmdir: '" + str(my_ret) + "'")
    return my_ret

def cleanup_staging(state):
    Logger.log_debug("ENTER cleanup_staging")
    my_ret = False
    cleanup_in_progress = False
    cleanup_process_obj = ""
    while(True):
        if not cleanup_in_progress:
            cleanup_process_obj = perform_fast_rmdir(state.source_dir)
            cleanup_in_progress = True
        else:
            if process_finished(cleanup_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(cleanup_process_obj)
    Logger.log_debug("EXIT cleanup_staging: '" + str(my_ret) + "'")
    return my_ret

def process_work(state):
    Logger.log_debug("ENTER process_work")
    while(True):
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
            if cleanup_staging(state):
                print_success(state)

        break
    Logger.log_debug("EXIT process_work")

process_running_added = False
try:
    if can_do_work():
        process_running_added = add_process_running()
        if process_running_added:
            my_work = get_work_available()
            if my_work:
                perform_heartbeat(my_work)
                spawn_new_worker(False)
                process_work(my_work)
           
except Exception as e:
    Logger.log_exception(e)
    spawn_new_worker(True)
finally:
     if process_running_added:
        remove_process_running()