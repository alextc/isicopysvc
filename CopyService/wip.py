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


work = look_for_work()

for folder in work:
    if not is_dir_in_progress(folder):
        if is_copy(folder):
            if claim_copy_dir_job(folder):
                #successfully claimed copy job
                start_copy_job(folder)
                 
        else:
            if claim_move_dir_job(folder):
                #successfully claimed move job
                job_id = start_ACL_repair_job(folder)
                set_ACL_repair_job(folder, job_id)
               

for folder in get_ACL_job_in_progress():
    get_isi_job_status()
    
            
      
          
         
        
    
