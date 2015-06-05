__author__ = 'alextc'
from isiapi.namespacecommand import NamespaceCommand
import logging
from aop.logstartandexit import LogEntryAndExit

class GetAclCommand(NamespaceCommand):
    def __init__(self, path):
        NamespaceCommand.__init__(self, http_verb="GET", directory=path, qry_dict={'acl': ''})
        self._path = path

    @LogEntryAndExit(logging.getLogger())
    def execute(self):
        logging.debug("about to execute GetAclCommand for {0}".format(self._path))
        raw_response = super(GetAclCommand, self).execute()
        if raw_response[2]:
            logging.debug("Returning {0}".format(raw_response[2]))
            return raw_response[2]

        logging.debug("Unable to get ACL for {0}".format(self._path))
        raise ValueError("Unable to get ACL for {0}".format(self._path))
