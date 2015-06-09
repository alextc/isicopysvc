import shutil
import logging
from work.processpool import ProcessPool
from work.workscheduler import WorkScheduler
from sql.heartbeatdb import HeartBeatDb
from fs.fsutils import FsUtils

FORMAT = "[%(asctime)s %(process)s %(message)s"
logging.basicConfig(filename='/ifs/copy_svc/wip.log',level=logging.DEBUG, format=FORMAT)

def start_workflow(work_item):
    logging.debug("ENTERING")
    # logging.debug("Received work item to process:\n{0}".format(work_item))

    if work_item.state == "ReAcl":
        FsUtils.reacl_tree(work_item.phase2_source_dir, work_item.acl_template_dir)
        logging.debug("Setting state to Move")
        work_item.state = "Move"

    if work_item.state == "Move":
        logging.debug("About to move {0} to {1}".format(work_item.phase2_source_dir, work_item.target_dir))
        shutil.move(work_item.phase2_source_dir, work_item.target_dir)
        logging.debug("Setting state to Cleanup")
        work_item.state = "Cleanup"

    if work_item.state == "Cleanup":
        # TODO: remove this comment below once phase1 is in place
        # shutil.rmtree(work_item.phase1_source_dir)
        HeartBeatDb().remove_work_item(work_item)
        return

    raise ValueError("Unexpected state of a work_item {0}", work_item)

if __name__ == '__main__':
    for i in range(500):
        process_pool = ProcessPool()
        if not process_pool.is_max_process_count_reached():
            my_work_item = WorkScheduler().try_get_new_phase2_work_item()
            if my_work_item:
                logging.debug("Got new work_item {0}".format(my_work_item))
                my_work_item.state = "ReAcl"
                start_workflow(my_work_item)

