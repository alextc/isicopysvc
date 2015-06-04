__author__ = 'alextc'
from isiapi.namespacecommand import NamespaceCommand
import json
import logging

class GetAclCommand(NamespaceCommand):
    def __init__(self, path):
        NamespaceCommand.__init__(self, "GET", ['namespace'] + path.split("/"), {'acl': ''})
        self._path = path

    def execute(self):
        logging.debug("Entered GetAclCommand Execute")
        raw_response = super(GetAclCommand, self).execute()
        if raw_response[2]:
            json_payload = json.loads(raw_response[2])
            logging.debug("JSON Paylod returend by ISI\n{0}".format(json_payload))
            return json_payload

        logging.debug("Unable to get ACL for {0}".format(self._path))
        raise ValueError("Unable to get ACL for {0}".format(self._path))