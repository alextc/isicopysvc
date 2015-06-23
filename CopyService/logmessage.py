#!/usr/bin/env python
import logging
import logging.handlers as handlers

if __name__ == '__main__':
        handler = handlers.SysLogHandler(address=('192.168.11.50', 514))
        formatter = logging.Formatter('%(name)s[%(process)d]: %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger("ISILog")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.debug("Hello World")
