__author__ = 'alextc'
import fcntl
import os
import sys

def lock_file_ex(file_to_lock, crash):
    if not os.path.exists(file_to_lock):
        raise ValueError("File does not exist '{}'".format(file_to_lock))
    handle = open(file_to_lock, 'a+')
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB )

    if crash == 'True' :
        raise ValueError("Crashing before releasing lock - this is part of the test")

if __name__ == "__main__":
    lock_file_ex(sys.argv[1], sys.argv[2])
