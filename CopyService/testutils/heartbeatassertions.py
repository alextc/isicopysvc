__author__ = 'alextc'
import socket
import os
import datetime
from sql.heartbeatdb import HeartBeatDb
from common.datetimeutils import DateTimeUtils


class HeartBeatAssertions(object):

    _max_allowed_heartbeat_drift = 2

    def __init__(self):
        self._heartbeatdb_phase1 = HeartBeatDb("phase1")
        self._heartbeatdb_phase2 = HeartBeatDb("phase2")

    def assert_heartbeat_phase1_was_written(self):
        latest_heartbeat = self._heartbeatdb_phase1.get_heartbeat(socket.gethostname(), os.getpid())
        assert latest_heartbeat, "Unable to get latest heartbeat phase1"
        now = datetime.datetime.now()
        assert DateTimeUtils.get_total_seconds_for_timedelta(
            datetime.datetime.now() - latest_heartbeat) < HeartBeatAssertions._max_allowed_heartbeat_drift,\
            "phase2 heartbeat is stale - unexpected. Time of check {0}, heartbeat was {1}".format(now, latest_heartbeat)

    def assert_heartbeat_phase2_was_written(self):
        latest_heartbeat = self._heartbeatdb_phase2.get_heartbeat(socket.gethostname(), os.getpid())
        assert latest_heartbeat, "Unable to get latest heartbeat phase2"
        now = datetime.datetime.now()
        assert DateTimeUtils.get_total_seconds_for_timedelta(
            now - latest_heartbeat) < HeartBeatAssertions._max_allowed_heartbeat_drift,\
            "phase2 heartbeat is stale - unexpected. Time of check {0}, heartbeat was {1}".format(now, latest_heartbeat)
