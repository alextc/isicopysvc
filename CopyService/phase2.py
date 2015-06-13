import shutil
import logging
from work.processpool import ProcessPool
from work.workscheduler import WorkScheduler
from sql.phase2db import Phase2Db
from fs.fsutils import FsUtils
from cluster.heartbeatmanager import HeartBeatManager
from model.phase2workitem import Phase2WorkItem


class Phase2(object):

    def __init__(self):
        format_logging = "[%(asctime)s %(process)s %(message)s"
        logging.basicConfig(filename='/ifs/copy_svc/wip.log', level=logging.DEBUG, format=format_logging)
        self.process_pool = ProcessPool()
        self.work_scheduler = WorkScheduler()
        self.heart_beat_db = Phase2Db()

    @staticmethod
    def start_workflow(work_item, heart_beat_manager):
        """
        :type work_item: Phase2WorkItem
        :type heart_beat_manager: HeartBeatManager
        :return:
        """
        logging.debug("ENTERING")
        # logging.debug("Received work item to process:\n{0}".format(work_item))

        if work_item.state == "ReAcl":
            FsUtils.reacl_tree(
                work_item.phase2_source_dir,
                work_item.acl_template_dir,
                heart_beat_manager)
            logging.debug("Setting state to Move")
            work_item.state = "Move"
            heart_beat_manager.write_heart_beat(force=True)

        if work_item.state == "Move":
            logging.debug("About to move {0} to {1}".format(work_item.phase2_source_dir, work_item.target_dir))
            shutil.move(work_item.phase2_source_dir, work_item.target_dir)
            logging.debug("Setting state to Cleanup")
            work_item.state = "Cleanup"
            heart_beat_manager.write_heart_beat(force=True)

        if work_item.state == "Cleanup":
            # I need to keep the record in the db so that the following racing condition is avoided:
            # one worker considers dir X as potential work but is suspended from CPU
            # second worker also considers X as potential work and remains on the CPU enough time to
            # claim it and complete work
            # If the second worker cleans the record from db, when the first one wakes up it may try to claim and will
            # succeed since the record in the db is no longer plays the role of a lock
            # heart_beat_manager.remove_work_item(work_item)
            return

        raise ValueError("Unexpected state of a work_item {0}", work_item)

if __name__ == '__main__':
    phase2 = Phase2()
    for i in range(500):
        if not phase2.process_pool.is_max_process_count_reached():
            my_work_item = phase2.work_scheduler.try_get_new_phase2_work_item()
            if my_work_item:
                logging.debug("Got new work_item {0}".format(my_work_item))
                my_work_item.state = "ReAcl"
                phase2.start_workflow(my_work_item, HeartBeatManager(phase2.heart_beat_db, my_work_item))