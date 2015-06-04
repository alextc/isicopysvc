__author__ = 'alextc'
from isiapi.namespacecommand import NamespaceCommand
import logging

class GetAclCommand(NamespaceCommand):
    def __init__(self, path):
        NamespaceCommand.__init__(self, http_verb="GET", directory=path, qry_dict={'acl': ''})
        self._path = path

    def execute(self):
        logging.debug("Entered GetAclCommand Execute")
        raw_response = super(GetAclCommand, self).execute()
        print raw_response[0]
        if raw_response[2]:
            return raw_response[2]

        logging.debug("Unable to get ACL for {0}".format(self._path))
        raise ValueError("Unable to get ACL for {0}".format(self._path))
