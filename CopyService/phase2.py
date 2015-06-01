__author__ = 'alextc'
from multiprocessing import Process
import time
import random


def worker():
    time.sleep(random.randint(1, 5))

if __name__ == '__main__':
    for num in range(5):
        p = Process(target=worker)
        p.start()
        print p.pid