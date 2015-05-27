import unittest
import fcntl

class file_locking_tests(unittest.TestCase):

    def test_attempt_to_lock_locked_file_must_throw_ioerror(self):
      with open("foo", 'a+') as file_to_lock:
            lock_file_ex(file_to_lock)
             
            with open("foo", 'a+') as file_to_lock_again:           
                self.failUnlessRaises(IOError, lock_file_ex, file_to_lock_again)

    def test_attempt_to_open_locked_file_for_write_succeeds(self):
      with open("foo", 'a+') as file_to_lock:
            lock_file_ex(file_to_lock)
             
            with open("foo", 'a+') as file_to_lock_again:           
                file_to_lock_again.writelines("Writing to a locked file\n")
                                
def lock_file_ex(file_handle):
        fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB )
                                       
if __name__ == '__main__':
    unittest.main()

