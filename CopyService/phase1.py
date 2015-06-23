__author__ = 'alextc'
from phase1work.phase1worker import Phase1Worker
from phase1work.phase1workscheduler import Phase1WorkScheduler
import logging


class Phase1(object):
    def __init__(self,):
        format_logging = "[%(asctime)s %(process)s %(message)s"
        logging.basicConfig(filename='/ifs/copy_svc/phase1.log', level=logging.DEBUG, format=format_logging)


if __name__ == '__main__':
    phase1_work_scheduler = Phase1WorkScheduler()
    phase1_worker = Phase1Worker()
    while 1:
        phase1_work_scheduler.run()
        phase1_worker.run()