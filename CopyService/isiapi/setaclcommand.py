__author__ = 'alextc'
from isiapi.namespacecommand import NamespaceCommand
import json
import logging

class SetAclCommand(NamespaceCommand):
    def __init__(self, path, acl):
        NamespaceCommand.__init__(self, "PUT", ['namespace'] + path.split("/"), {'acl': ''}, acl)
        self._path = path

    def execute(self):
        logging.debug("Entered SetAclCommand Execute")
        raw_response = super(SetAclCommand, self).execute()
        if raw_response[2]:
            json_payload = json.loads(raw_response[2])
            logging.debug("JSON Paylod returend by ISI\n{0}".format(json_payload))
            return json_payload

        logging.debug("Unable to get ACL for {0}".format(self._path))
        raise ValueError("Unable to get ACL for {0}".format(self._path))
