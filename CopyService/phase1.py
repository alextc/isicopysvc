__author__ = 'alextc'
from work.phase1worker import Phase1Worker
from work.phase1workscheduler import Phase1WorkScheduler
import logging


class Phase1(object):
    def __init__(self,):
        format_logging = "[%(asctime)s %(process)s %(message)s"
        logging.basicConfig(filename='/ifs/copy_svc/phase1.log', level=logging.DEBUG, format=format_logging)


if __name__ == '__main__':
    while 1:
        Phase1WorkScheduler().run()
        Phase1Worker().run()