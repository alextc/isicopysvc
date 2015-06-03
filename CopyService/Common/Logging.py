import ConfigParser
import logging

settings = ConfigParser.ConfigParser()
settings.read('/ifs/copy_svc/config')

LEVELS = {'DEBUG': 10, 
          'INFO': 20,
          'WARNING': 30,
          'ERROR': 40,
          'CRITICAL': 50}

log_level = LEVELS[settings.get('Logging', 'Level')]
logging.basicConfig(level=log_level,)

def log_debug(string):
    logging.debug(string)

def log_info(string):
    logging.info(string)

def log_warning(string):
    logging.warning(string)

def log_error(string):
    logging.error(string)

def log_critical(string):
    logging.critical(string)

def log_exception(exception):
    logging.exception(exception)
