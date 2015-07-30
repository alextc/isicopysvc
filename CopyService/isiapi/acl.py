__author__ = 'alextc'
import json
from .ace import Ace
from log.loggerfactory import LoggerFactory


class Acl(object):

    _logger = LoggerFactory().create("Acl")

    def __init__(self, get_acl_raw_output=None, aces=None):
        assert not (get_acl_raw_output and aces), "Supply either raw_acl or constructed aces"

        if get_acl_raw_output:
            json_acl = json.loads(get_acl_raw_output)
            self.aces = []
            for ace in json_acl['acl']:
                self.aces.append(Ace(ace['trustee']['id'], ace['accessrights']))
        else:
            self.aces = aces

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            Acl._logger.debug("Equality check failed based on type check")
            return False

        # TODO: Current check depends on the order of aces. Use set (convert list to set) to remove this dependency
        if self.aces != other.aces:
            Acl._logger.debug("Equality check failed based on aces check")
            return False

        return True

    def __str__(self):
        result = ''
        for ace in self.aces:
            result += str(ace)
            result += '\n'

        return result