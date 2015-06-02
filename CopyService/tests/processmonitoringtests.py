__author__ = 'alextc'
import unittest
from multiprocessing import Process
import time

class ProcessMonitoringTests(unittest.TestCase):

    def worker(self, time_to_sleep):
        time.sleep(time_to_sleep)

    def test_worker_process_must_terminate_once_work_is_complete(self):
        time_to_sleep = 2
        proc = Process(target=self.worker, args=(time_to_sleep,))
        proc.start()
        self.assertTrue(proc.is_alive())
        time.sleep(time_to_sleep + 2)
        self.assertFalse(proc.is_alive())

if __name__ == '__main__':
    unittest.main()

