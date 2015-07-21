#!/usr/bin/env python
from phase1work.processpool import ProcessPool
from phase2work.phase2workscheduler import Phase2WorkScheduler
from sql.phase2db import Phase2Db
from cluster.phase2workitemheartbeatmanager import Phase2WorkItemHeartBeatManager
from phase2work.phase2worker import Phase2Worker


class Phase2(object):
    def __init__(self):
        self.process_pool = ProcessPool()
        self.worker = Phase2Worker()
        self.work_scheduler = Phase2WorkScheduler()
        self.heart_beat_db = Phase2Db()

if __name__ == '__main__':
    phase2 = Phase2()
    for i in range(500):
        my_work_item = phase2.work_scheduler.try_get_new_phase2_work_item()
        if my_work_item:
            my_work_item.state = "ReAcl"
            phase2.worker.run(my_work_item, Phase2WorkItemHeartBeatManager(phase2.heart_beat_db, my_work_item))
            # TODO: Add my_work_item.validate()