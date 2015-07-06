__author__ = 'alextc'
import time
from isiapi.namespacecommand import NamespaceCommand
from log.loggerfactory import LoggerFactory


class SetAclCommand(NamespaceCommand):
    _function_call_count = 0
    _retry_max_count = 10
    _retry_back_off_in_sec = 1
    _logger = LoggerFactory.create('SetAclCommand')

    def __init__(self, path, acl):

        SetAclCommand._logger.debug("Path to ACL:{0}".format(path))
        SetAclCommand._logger.debug("ACL to apply:\n{0}".format(acl))
        NamespaceCommand.__init__(self, http_verb="PUT", directory=path, qry_dict={'acl': ''}, body_data=acl)
        self._path = path

    def execute(self):
        for i in range(SetAclCommand._retry_max_count):
            SetAclCommand._function_call_count += 1
            try:
                raw_response = super(SetAclCommand, self).execute()
                SetAclCommand._logger.debug(
                    "Set ACL API for dir {0} completed with the status of {1} and message body of {2}".
                    format(self._path, raw_response[0], raw_response[1]))
                assert raw_response[0] == 200, "Set ACL API for dir {0} failed,response was {1}, message body was {2}".\
                    format(self._path, raw_response[0], raw_response[1])
                return
            except RuntimeError as e:
                SetAclCommand._logger.debug(
                    "{0}, while processing {1}".format(e, self._path))
                SetAclCommand._logger.debug(
                    "The function was called {0} times".format(SetAclCommand._function_call_count))
                time.sleep(SetAclCommand._retry_back_off_in_sec)

        SetAclCommand._logger.debug("Maximum retry count reached; failing")
        raise RuntimeError("Unable to complete SetACLCommand")
