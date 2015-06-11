__author__ = 'alextc'
from sql.heartbeatdb import HeartBeatDb
from model.phase2workitem import Phase2WorkItem
import datetime
import logging
from aop.logstartandexit import LogEntryAndExit
from common.datetimeutils import DateTimeUtils


class HeartBeatManager(object):
    def __init__(self, heart_beat_db, phase2_work_item):
        """
        :type heart_beat_db: HeartBeatDb
        :type phase2_work_item: Phase2WorkItem
        :return:
        """
        self._heart_beat_db = heart_beat_db
        self._phase2_work_item = phase2_work_item
        self._last_heart_beat = None

    @LogEntryAndExit(logging.getLogger())
    def write_heart_beat(self, force=False):
        if force or self._should_write_heart_beat():
            self._phase2_work_item.heartbeat = datetime.datetime.now()
            self._heart_beat_db.write_heart_beat(self._phase2_work_item)
            self._last_heart_beat = self._phase2_work_item.heartbeat
            logging.debug("Wrote heart beat for\n{0}".format(self._phase2_work_item))

    def get_heart_beat(self):
        return self._heart_beat_db.get_heart_beat(self._phase2_work_item.phase2_source_dir)

    def try_to_take_ownership_of_heart_beating(self):
        self._phase2_work_item.heartbeat = datetime.datetime.now()
        if self._heart_beat_db.try_to_take_ownership(self._phase2_work_item):
            self._last_heart_beat = self._phase2_work_item.heartbeat
            return True

        return False

    def try_to_remove_stale_heart_beat_record(self):
        self._heart_beat_db.remove_work_item(self._phase2_work_item)
        if not self._heart_beat_db.get_heart_beat(self._phase2_work_item.phase2_source_dir):
            return True
        else:
            return False

    @LogEntryAndExit(logging.getLogger())
    def _should_write_heart_beat(self):
        """
        :rtype: bool
        """

        # First time writing heartbeat
        if not self._last_heart_beat:
            logging.debug("_should_write_heart_beat returning True")
            return True

        seconds_since_last_heartbeat = \
            DateTimeUtils.get_total_seconds_for_timedelta(datetime.datetime.now() - self._last_heart_beat)
        result = seconds_since_last_heartbeat > (Phase2WorkItem.heart_beat_max_threshold_in_sec / 2)
        logging.debug("_should_write_heart_beat returning {0}".format(result))
        return result
