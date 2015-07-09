__author__ = 'alextc'
import ConfigParser

class ConfigurableObject(object):
    _config = ConfigParser.RawConfigParser()
    _config.read('config')
