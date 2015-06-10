__author__ = 'alextc'
from sql.heartbeatdb import HeartBeatDb
from model.phase2workitem import Phase2WorkItem
import datetime
import logging
from aop.logstartandexit import LogEntryAndExit


class HeartBeatManager(object):
    _heart_beat_max_threshold_in_sec = 5

    def __init__(self, heart_beat_db, phase2_work_item):
        """
        :type heart_beat_db: HeartBeatDb
        :type phase2_work_item: Phase2WorkItem
        :return:
        """
        self._heart_beat_db = heart_beat_db
        self._phase2_work_item = phase2_work_item
        self._last_heart_beat = None

    def write_heart_beat(self):
        assert self._get_total_seconds_for_timedelta(datetime.datetime.now() - self._phase2_work_item.heartbeat) < \
               HeartBeatManager._heart_beat_max_threshold_in_sec, \
               "Attempt to write a heartbeat after expiration threshold"

        if self._should_write_heart_beat():
            self._phase2_work_item.heartbeat = datetime.datetime.now()
            self._heart_beat_db.write_heart_beat(self._phase2_work_item)
            self._last_heart_beat = self._phase2_work_item.heartbeat

    def try_to_take_ownership_of_heart_beating(self):
        self._phase2_work_item.heartbeat = datetime.datetime.now()
        if self._heart_beat_db.try_to_take_ownership(self._phase2_work_item):
            self._last_heart_beat = self._phase2_work_item.heartbeat
            return True

    def force_ownership_takeover_of_heart_beating(self):
        self._phase2_work_item.heartbeat = datetime.datetime.now()
        self._heart_beat_db.force_ownership_take_over(self._phase2_work_item)
        self._last_heart_beat = self._phase2_work_item.heartbeat

    def is_heart_beat_stale(self):
        heart_beat = self._heart_beat_db.get_heart_beat(self._phase2_work_item.phase2_source_dir).heartbeat
        assert heart_beat < datetime.datetime.now(), "heartbeat can't be in the future"
        is_stale = self._get_total_seconds_for_timedelta((datetime.datetime.now() - heart_beat)) > \
                   HeartBeatManager._heart_beat_max_threshold_in_sec
        return is_stale

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
            self._get_total_seconds_for_timedelta(datetime.datetime.now() - self._last_heart_beat)
        result = seconds_since_last_heartbeat > (HeartBeatManager._heart_beat_max_threshold_in_sec / 2)
        logging.debug("_should_write_heart_beat returning {0}".format(result))
        return result

    # Required on Python 2.6 since total_seconds attribute was added in 2.7
    @LogEntryAndExit(logging.getLogger())
    def _get_total_seconds_for_timedelta(self, timedelta):
        """
        :type timedelta: datetime.timedelta
        :rtype: int
        """
        logging.debug("About to calculate delta_in_seconds for {0}".format(timedelta))
        delta_in_seconds = timedelta.days * 86400 + timedelta.seconds
        logging.debug("_get_total_seconds_for_time_delta is returning: {0}".format(delta_in_seconds))
        return delta_in_seconds
