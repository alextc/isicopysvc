__author__ = 'alextc'
from log.loggerfactory import LoggerFactory
from isiapi.namespacecommand import NamespaceCommand


class GetAclCommand(NamespaceCommand):

    _logger = LoggerFactory().create("GetAclCommand")

    def __init__(self, path):
        NamespaceCommand.__init__(self, http_verb="GET", directory=path, qry_dict={'acl': ''})
        self._path = path

    def execute(self):
        GetAclCommand._logger.debug("about to execute GetAclCommand for {0}".format(self._path))
        raw_response = super(GetAclCommand, self).execute()
        if raw_response[2]:
            GetAclCommand._logger.debug("Returning {0}".format(raw_response[2]))
            return raw_response[2]

        GetAclCommand._logger.debug("Unable to get ACL for {0}".format(self._path))
        raise ValueError("Unable to get ACL for {0}".format(self._path))
