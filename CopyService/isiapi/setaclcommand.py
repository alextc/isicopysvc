__author__ = 'alextc'
import time
from isiapi.namespacecommand import NamespaceCommand
import logging
from aop.logstartandexit import LogEntryAndExit

class SetAclCommand(NamespaceCommand):
    _function_call_count = 0
    _retry_max_count = 10
    _retry_back_off_in_sec = 1

    def __init__(self, path, acl):
        NamespaceCommand.__init__(self, http_verb="PUT", directory=path, qry_dict={'acl': ''}, body_data=acl)
        self._path = path

    @LogEntryAndExit(logging.getLogger())
    def execute(self):
        retry_count = 0

        while True:
            SetAclCommand._function_call_count += 1
            try:
                raw_response = super(SetAclCommand, self).execute()
                assert raw_response[0] == 200, "Set ACL API failed"
                logging.debug("Set ACL API for dir {0} completed with the status of {1}".format(
                    self._path,
                    raw_response[0]))
                return
            except RuntimeError as e:
                logging.debug("{0}, while processing {1}".format(e, self._path))
                logging.debug("The function was called {0} times".format(SetAclCommand._function_call_count))
                retry_count += 1
                if retry_count > SetAclCommand._retry_max_count:
                    logging.debug("Maximum retry count reached; failing")
                    raise
                else:
                    time.sleep(SetAclCommand._retry_back_off_in_sec)
