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
from CopyService.aop.logstartandexit import LogEntryAndExit

max_retry_count = 5
max_concurrent = 5
max_cpu_load = 50
max_stale_hb_time_in_seconds = 30
process_file = '/ifs/copy_svc/' + socket.gethostname() + '_process_list.dat'
process_file_persist = '/ifs/copy_svc/' + socket.gethostname() + '_process_list_persist.dat'
potential_work_target_string = "/ifs/zones/*/copy_svc/staging/*/*"
datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'
process_state = {"Init":"Init", "CopyOrig":"CopyOrig", "ReAcl":"ReAcl", "Move":"Move", "Cleanup":"Cleanup"}

logging.basicConfig(filename='wip.log',level=logging.DEBUG)

class state_obj:
    cur_state = "Init"
    source_dir = ""
    target_dir = ""
    process_dir = ""
    hb_file = ""
    state_file = ""
    def __str__(self):
        return "State:" + self.cur_state + "\n" + "Source:" + self.source_dir + "\n" + "Target" + self.target_dir + "\n" + "ProcessDir:" + self.process_dir + "\n" + "HeartBeatFile:" + self.hb_file + "\n" + "StateFile:" + self.state_file

@LogEntryAndExit(logging.getLogger())
def is_max_process_count_reached():
    if not os.path.isfile(process_file):
        raise ValueError("Process file does not exist")

    for i in range(max_retry_count):
        try:
            with open(process_file) as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                processes = process_info.readlines()
                if processes:
                    logging.debug("current_process_count: " +
                                  str(len(processes)) +
                                  ", MaxConcurrent: " +
                                  str(max_concurrent) +
                                  " spawn_new? " +
                                  str(len(processes) < max_concurrent) + "\n")
                    return len(processes) >= max_concurrent
                else:
                    # Empty file - could it be?
                    return False
        except:
            logging.debug(sys.exc_info()[0])
            time.sleep(1)

    if i == (max_retry_count -1):
        logging.debug("Unable to open process file in max_process_running")
        raise ValueError("Unable to open process file")

@LogEntryAndExit(logging.getLogger())
def add_process_running():
    for i in range(max_retry_count):
        try:
            with open(process_file, 'a+') as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                process_info.writelines(str(os.getpid()) + "\n")
            return
        except:
           logging.debug(sys.exc_info()[0])
           time.sleep(1)

    if i == (max_retry_count -1):
        logging.debug("Unable to update process count in add_process_running")
        raise ValueError("Unable to open process file")

@LogEntryAndExit(logging.getLogger())
def remove_process_running():
    for i in range(max_retry_count):
        try:
            with open(process_file, 'r') as process_info:
                fcntl.flock(process_info.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lines = process_info.readlines()
            if lines:
                lines.pop()
                with open(process_file,'w') as process_info:
                    fcntl.flock(process_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    process_info.writelines(lines)
            else:
                logging.debug("Was asked to remove a process record from an empty file")
                raise ValueError("Was asked to remove a process record from an empty file")
        except:
            logging.debug(sys.exc_info()[0])
            time.sleep(1)

    if i == (max_retry_count -1):
        logging.debug("Unable to update process count in add_process_running")
        raise ValueError("Unable to open process file")


@LogEntryAndExit(logging.getLogger())
def can_do_work():
    return is_max_process_count_reached()

@LogEntryAndExit(logging.getLogger())
def get_work_available():
    stranded_work = get_stranded_work()
    if stranded_work:
       return stranded_work()

    return get_new_work

@LogEntryAndExit(logging.getLogger())
def get_new_work():
    potential_work_targets = glob.glob(potential_work_target_string)
    potential_work_targets.sort()
    if potential_work_targets:
        for each_potential_work_target in potential_work_targets:
            if not each_potential_work_target.endswith("_in_process"):
                ownership_state = take_ownership(each_potential_work_target, False)
                if ownership_state:
                    return ownership_state

@LogEntryAndExit(logging.getLogger())
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
            potential_takes.sort()
            if not potential_takes or ignore_prev_owner :
               if not os.path.exists(expected_path):
                   try:
                       os.mkdir(expected_path)
                   except IOError as e:
                       Logger.log_exception(e)
                       break
               if (not os.path.exists(expected_owner_file)) or ignore_prev_owner:
                   with open(expected_owner_file, 'w+') as owner_file:
                       fcntl.flock(expected_owner_file.fileno(), fcntl.LOCK_EX)
                       owner_file.writelines(socket.gethostname() + ":" + str(os.getpid()))
               else:
                   Logger.log_warning("owner file already existed for file '" + expected_owner_file + "'")
                   break
               with open(expected_hb_file,'w+') as hb_file:
                   hb_file.writelines(datetime.datetime.utcnow().strftime(datetime_format_string))
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

@LogEntryAndExit(logging.getLogger())
def get_target_from_source(source_dir):
    Logger.log_debug("ENTER get_target_from_source")
    my_ret = source_dir.replace("staging","from")
    Logger.log_debug("EXIT get_target_from_source: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def get_stranded_work():
    Logger.log_debug("ENTER get_stranded_work")
    my_ret = None
    potential_strand_targets = glob.glob(potential_work_target_string)
    potential_strand_targets.sort()
    if potential_strand_targets:
        for each_potential_strand_target in potential_strand_targets:
            if each_potential_strand_target.endswith("_in_process"):
                if stale_heartbeat(each_potential_strand_target):
                    my_ret = take_ownership(get_original_source(each_potential_strand_target), True)
                    if my_ret:
                        my_ret.cur_state = get_state(each_potential_strand_target)
                        break
    Logger.log_debug("EXIT get_stranded_work: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def get_original_source(ownership_path):
    Logger.log_debug("ENTER get_original_source")
    my_ret = ""
    original_source = ownership_path + "/source.dat"
    if os.path.exists(original_source):
        with open(original_source) as orig_source:
            my_ret = orig_source.readline().strip()
    Logger.log_debug("EXIT get_original_source: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def stale_heartbeat(ownership_path):
    Logger.log_debug("ENTER stale_heartbeat")
    my_ret = True
    hbFile = ownership_path + "/hb.dat"
    start = datetime.datetime.utcnow()
    while(True):
        if os.path.exists(hbFile):
            with open(hbFile) as last_heartbeat:
                fcntl.flock(last_heartbeat.fileno(), fcntl.LOCK_EX)
                last_hb_time = last_heartbeat.readline().strip()
                if last_hb_time:
                    last_hb_datetime = datetime.datetime.strptime(last_hb_time, datetime_format_string)
                    if (datetime.datetime.utcnow() - last_hb_datetime).seconds < max_stale_hb_time_in_seconds:
                        my_ret = False
            break
        else:
            #sleep for a second to let the other process try to write the file
            time.sleep(1)
            if (datetime.datetime.utcnow() - start).seconds > max_stale_hb_time_in_seconds:
                #exceeded 15 seconds waiting for the hb file
                break

    Logger.log_debug("EXIT stale_heartbeat: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def get_state(ownership_path):
    Logger.log_debug("ENTER get_state")
    my_ret = "Init"
    state_file_name = ownership_path + "/state.dat"
    if os.path.exists(state_file_name):
        with open(state_file_name) as state_file:
            my_ret = state_file.readline()
       
    Logger.log_debug("EXIT get_state: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def get_ownership_path(path):
    Logger.log_debug("ENTER get_ownership_path")
    my_ret = path + "_in_process"
    Logger.log_debug("EXIT get_ownership_path: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def launch_script(script_path):
    os.system("python '" + script_path + "'")

@LogEntryAndExit(logging.getLogger())
def spawn_new_worker(should_wait):
    Logger.log_debug("ENTER spawn_new_worker")
    process_obj = Process(target=launch_script, args=(os.path.realpath(__file__),))
    process_obj.start()
    if should_wait:
        Logger.log_debug("spawning new instance of script and waiting")
        process_obj.join()
    else:
        Logger.log_debug("spawning new instance of script and not waiting")

    Logger.log_debug("EXIT spawn_new_worker")

@LogEntryAndExit(logging.getLogger())
def perform_heartbeat(state):
    Logger.log_debug("ENTER perform_heartbeat")
    with open(state.hb_file,'w+') as hb_file:
        fcntl.flock(hb_file.fileno(), fcntl.LOCK_EX)
        cur_hb_time_str = datetime.datetime.utcnow().strftime(datetime_format_string)
        Logger.log_debug("Current HB time: '" + cur_hb_time_str + "'")
        hb_file.writelines(cur_hb_time_str)
    Logger.log_debug("EXIT perform_heartbeat")

@LogEntryAndExit(logging.getLogger())
def needs_copy(state):
    Logger.log_debug("ENTER needs_copy")
    my_ret = False
    perform_heartbeat(state)
    if os.path.exists(state.target_dir):
        my_ret = True

    Logger.log_debug("EXIT needs_copy: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def save_state(state):
    Logger.log_debug("ENTER save_state")
    perform_heartbeat(state)
    with open(state.state_file,'w+') as state_file:
        state_file.writelines(state.cur_state)
    Logger.log_debug("EXIT save_state")

@LogEntryAndExit(logging.getLogger())
def ascyn_fast_copy(source_dir, target_dir):
    Logger.log_debug("ENTER ascyn_fast_copy")
    my_ret = False

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
    my_ret = True
    Logger.log_debug("EXIT ascyn_fast_copy: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def perform_fast_copy(source_dir, target_dir):
    Logger.log_debug("ENTER perform_fast_copy")
    my_ret = None
    my_ret = Process(target=ascyn_fast_copy, args=(source_dir,target_dir))
    my_ret.start()
    Logger.log_debug("EXIT perform_fast_copy: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def process_finished(process_obj):
    Logger.log_debug("ENTER process_finished")
    my_ret = True
    if process_obj.is_alive():
       my_ret = False
    Logger.log_debug("EXIT process_finished: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def check_process_result(process_obj):
    Logger.log_debug("ENTER check_process_result")
    my_ret = False
    if process_obj:
        my_ret = (process_obj.exitcode == 0)
    Logger.log_debug("EXIT check_process_result: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def copy_original_to_staging(state):
    Logger.log_debug("ENTER copy_original_to_staging")
    my_ret = False
    copy_in_progress = False
    copy_process_obj = ""
    while(True):
        perform_heartbeat(state)
        if not copy_in_progress:
            copy_process_obj = perform_fast_copy(state.target_dir, state.source_dir)
            copy_in_progress = True
        else:
            if process_finished(copy_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(copy_process_obj)
    Logger.log_debug("EXIT copy_original_to_staging: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def get_source_acls(source_dir):
    Logger.log_debug("ENTER get_source_acls")
    my_ret = None
    acl_string = PAPI.grab_aclfromobj(source_dir)
    if acl_string:
        my_ret = json.loads(acl_string)
    Logger.log_debug("EXIT get_source_acls: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def set_dest_acls(source_dir, acl):
    Logger.log_debug("ENTER set_dest_acls")
    my_ret = PAPI.set_aclonobj(source_dir, acl)
    Logger.log_debug("EXIT set_dest_acls: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def async_reacl(source_dir, dest_dir):
    Logger.log_debug("ENTER async_reacl")
    Logger.log_debug(source_dir)
    Logger.log_debug(dest_dir)
    my_ret = False
    error_hit = False

    acls = get_source_acls(source_dir)
    result = set_dest_acls(dest_dir, acls)
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

    Logger.log_debug("EXIT async_reacl: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def perform_fast_reacl(source_dir, dest_dir):
    Logger.log_debug("ENTER perform_fast_reacl")
    Logger.log_debug(source_dir)
    Logger.log_debug(dest_dir)
    my_ret = None
    my_ret = Process(target=async_reacl, args=(source_dir, dest_dir))
    my_ret.start()
    Logger.log_debug("EXIT perform_fast_reacl: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def reacl_staging(state):
    Logger.log_debug("ENTER reacl_staging")
    my_ret = False
    reacl_in_progress = False
    reacl_process_obj = ""
    while(True):
        perform_heartbeat(state)
        if not reacl_in_progress:
            reacl_process_obj = perform_fast_reacl(os.path.split(state.target_dir)[0], state.source_dir)
            reacl_in_progress = True
        else:
            if process_finished(reacl_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(reacl_process_obj)
    Logger.log_debug("EXIT reacl_staging: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def async_move(state):
    Logger.log_debug("ENTER async_move")
    my_ret = False
    if os.path.exists(state.target_dir):
        shutil.move(state.target_dir, state.process_dir + "/old_source_dir")

    shutil.move(state.source_dir, state.target_dir)
    my_ret = True
    Logger.log_debug("EXIT async_move: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def perform_fast_move(state):
    Logger.log_debug("ENTER perform_fast_move")
    my_ret = None
    my_ret = Process(target=async_move, args=(state,))
    my_ret.start()
    Logger.log_debug("EXIT perform_fast_move: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def move_staging(state):
    Logger.log_debug("ENTER move_staging")
    my_ret = False
    move_in_progress = False
    move_process_obj = ""
    while(True):
        perform_heartbeat(state)
        if not move_in_progress:
            move_process_obj = perform_fast_move(state)
            move_in_progress = True
        else:
            if process_finished(move_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(move_process_obj)
    Logger.log_debug("EXIT move_staging: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def async_rmdir(dir):
    Logger.log_debug("ENTER async_rmdir")

    shutil.rmtree(dir)

    Logger.log_debug("EXIT async_rmdir")

@LogEntryAndExit(logging.getLogger())
def perform_fast_rmdir(source_dir):
    Logger.log_debug("ENTER perform_fast_rmdir")
    my_ret = False

    my_ret = None
    my_ret = Process(target=async_rmdir, args=(source_dir,))
    my_ret.start()
    Logger.log_debug("EXIT perform_fast_rmdir: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def cleanup_staging(state):
    Logger.log_debug("ENTER cleanup_staging")
    my_ret = False
    cleanup_in_progress = False
    cleanup_process_obj = ""
    while(True):
        if not cleanup_in_progress:
            cleanup_process_obj = perform_fast_rmdir(state.process_dir)
            cleanup_in_progress = True
        else:
            if process_finished(cleanup_process_obj):
                break
        time.sleep(1)

    my_ret = check_process_result(cleanup_process_obj)
    Logger.log_debug("EXIT cleanup_staging: '" + str(my_ret) + "'")
    return my_ret

@LogEntryAndExit(logging.getLogger())
def print_success(state):
    Logger.log_debug("ENTER print_success")
    Logger.log_message("SUCCESS moved: '" + state.source_dir + "' to '" + state.target_dir + "'")
    Logger.log_debug("EXIT print_success")
@LogEntryAndExit(logging.getLogger())
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

@LogEntryAndExit(logging.getLogger())
if __name__ == '__main__':
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
    except KeyboardInterrupt:
        logging.debug("ctrl-c detected")
    except Exception as e:
        logging.debug(e)
        raise
    finally:
        if process_running_added:
            remove_process_running()
