import os
import shutil
import logging
from work.processpool import ProcessPool
from work.workscheduler import WorkScheduler
from sql.heartbeatdb import HeartBeatDb
from fs.fsutils import FsUtils

FORMAT = "[%(asctime)s %(process)s %(message)s"
logging.basicConfig(filename='/ifs/copy_svc/wip.log',level=logging.DEBUG, format=FORMAT)

def async_move(state):
    if os.path.exists(state.target_dir):
        shutil.move(state.target_dir, state.process_dir + "/old_source_dir")

    shutil.move(state.source_dir, state.target_dir)
    my_ret = True
    return my_ret

def start_workflow(work_item):
    logging.debug("ENTERING")
    # logging.debug("Received work item to process:\n{0}".format(work_item))

    if work_item.state == "ReAcl":
        logging.debug("Setting state to Cleanup")
        FsUtils.reacl_tree(work_item.source_dir, work_item.acl_template_dir)
        work_item.state = "Cleanup"

    if work_item.state == "Cleanup":
        shutil.rmtree(work_item.source_dir)
        HeartBeatDb().remove_work_item(work_item)
        return

    raise ValueError("Unexpected state of a work_item {0}", work_item)

if __name__ == '__main__':
    for i in range(5000):
        process_pool = ProcessPool()
        if not process_pool.is_max_process_count_reached():
            my_work_item = WorkScheduler().try_get_new_phase2_work_item()
            if my_work_item:
                logging.debug("Got new work_item {0}".format(my_work_item))
                my_work_item.state = "ReAcl"
                start_workflow(my_work_item)

