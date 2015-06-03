__author__ = 'alextc'
from lnx.processlist import ProcessList

ps = ProcessList()
ps.get_number_of_running_scripts("sleep.py")