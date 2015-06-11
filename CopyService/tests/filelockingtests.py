import unittest
import fcntl
import subprocess
import time


class FileLockingTests(unittest.TestCase):

    def test_attempt_to_lock_locked_file_must_throw_ioerror(self):
        with open("foo", 'a+') as file_to_lock:
            lock_file_ex(file_to_lock)
             
            with open("foo", 'a+') as file_to_lock_again:           
                self.failUnlessRaises(IOError, lock_file_ex, file_to_lock_again)

    def test_attempt_to_open_locked_file_for_write_succeeds(self):
        with open("foo", 'a+') as file_to_lock:
            lock_file_ex(file_to_lock)
             
            # I would expect this to fail since the file is locked, but this works
            with open("foo", 'a+') as file_to_lock_again:           
                file_to_lock_again.writelines("Writing to a locked file\n")

    def test_must_aquire_lock_after_lock_is_released(self):
        with open("foo1", 'a+') as file_to_lock1:
            lock_file_ex(file_to_lock1)

        with open("foo1", 'a+') as file_to_lock2:
            lock_file_ex(file_to_lock2)

    # TODO: Redo this tests with Process instead of subprocess
    def test_must_get_lock_when_another_process_crashed_before_releasing(self):
        subprocess.call(
            ['python', '/ifs/copy_svc/code/CopyService/utils/lockfile.py', '/ifs/copy_svc/lock1.file', 'True'])

        time.sleep(1)

        try:
            subprocess.check_call(
                ['python', '/ifs/copy_svc/code/CopyService/utils/lockfile.py', '/ifs/copy_svc/lock1.file', 'False'])
        except subprocess.CalledProcessError as err:
            print 'ERROR:', err
            raise


def lock_file_ex(file_handle):
        # Calling this without LOCK_NB makes this a blocking call
        fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                                       
if __name__ == '__main__':
    unittest.main()

