__author__ = 'alextc'
import unittest
import logging
import logging.handlers as handlers


class SyslogTests(unittest.TestCase):

    def must_send_message_to_remote_syslog_server(self):
        handler = handlers.SysLogHandler(address=('192.168.11.50', 514))
        formatter = logging.Formatter('%(name)s[%(process)d]: %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger("ISILog")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.debug("Hello World")


if __name__ == '__main__':
    unittest.main()
