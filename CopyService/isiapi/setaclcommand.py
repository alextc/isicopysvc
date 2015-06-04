__author__ = 'alextc'
from isiapi.namespacecommand import NamespaceCommand
import logging

class SetAclCommand(NamespaceCommand):
    def __init__(self, path, acl):
        NamespaceCommand.__init__(self, http_verb="PUT", directory=path, qry_dict={'acl': ''}, body_data=acl)
        self._path = path

    def execute(self):
        logging.debug("Entered SetAclCommand Execute")
        raw_response = super(SetAclCommand, self).execute()
        assert raw_response[0] == 200, "Set ACL API failed"
        logging.debug("Set ACL API for dir {0} completed with the status of {1}".format(self._path, raw_response))
