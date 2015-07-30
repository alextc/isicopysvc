__author__ = 'alextc'
from log.loggerfactory import LoggerFactory


class Ace(object):

    _logger = LoggerFactory().create("Ace")

    def __init__(self, trustee, rights):
        self.trustee = trustee
        self.rights = rights

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            Ace._logger.debug("Equality check failed based on type check")
            return False

        if self.trustee != other.trustee:
            Ace._logger.debug("Equality check failed based on trustee check")
            Ace._logger.debug("self.trustee {0}, other.trustee{1}".format(self.trustee, other.trustee))
            return False

        # TODO: Current check depends on the order of rights. Use set (convert list to set) to remove this dependency
        if self.rights != other.rights:
            Ace._logger.debug("Equality check failed based on ACE check")
            Ace._logger.debug("Self is {0}".format(self))
            Ace._logger.debug("Other is {0}".format(other))
            return False

        return True

    def __str__(self):
        result = ''
        result += 'Trustee: {0}\n'.format(self.trustee)
        result += "Rights:\n"
        for right in self.rights:
            result += right
            result += '\n'

        return result