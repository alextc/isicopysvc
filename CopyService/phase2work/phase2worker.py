__author__ = 'alextc'
import shutil
import logging
from fs.fsutils import FsUtils
from cluster.phase2workitemheartbeatmanager import Phase2WorkItemHeartBeatManager
from model.phase2workitem import Phase2WorkItem
from log.loggerfactory import LoggerFactory


class Phase2Worker(object):

    _logger = LoggerFactory().create("Phase2Worker")

    def __init__(self):
        pass

    def run(self, work_item, heart_beat_manager):
        """
        :type work_item: Phase2WorkItem
        :type heart_beat_manager: Phase2WorkItemHeartBeatManager
        :return:
        """
        self._logger.debug("Received phase1work item to process:\n{0}".format(work_item))

        if work_item.state == "ReAcl":
            Phase2Worker._logger.debug("Received work_item with with state==ReAcl\n{0}".format(work_item))
            FsUtils().reacl_tree(
                work_item.phase2_source_dir,
                work_item.acl_template_dir,
                heart_beat_manager)
            Phase2Worker._logger.debug("Setting state to Move")
            work_item.state = "Move"
            heart_beat_manager.write_heart_beat(force=True)

        if work_item.state == "Move":
            Phase2Worker._logger.debug(
                "About to move {0} to {1}".format(work_item.phase2_source_dir, work_item.target_dir))
            shutil.move(work_item.phase2_source_dir, work_item.target_dir)
            logging.debug("Setting state to Cleanup")
            work_item.state = "Cleanup"
            heart_beat_manager.write_heart_beat(force=True)

        if work_item.state == "Cleanup":
            # I need to keep the record in the db so that the following racing condition is avoided:
            # one worker considers dir X as potential phase1work but is suspended from CPU
            # second worker also considers X as potential phase1work and remains on the CPU enough time to
            # claim it and complete phase1work
            # If the second worker cleans the record from db, when the first one wakes up it may try to claim and will
            # succeed since the record in the db is no longer plays the role of a lock
            # heart_beat_manager.remove_work_item(work_item)
            return

        raise ValueError("Unexpected state of a work_item {0}", work_item)
