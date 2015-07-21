__author__ = 'alextc'

import os
import datetime
import logging
import socket
from log.loggerfactory import LoggerFactory
from sqlitefacade import SqliteFacade


class HeartBeatDb:
    phase1db_data_file_path = "/ifs/copy_svc/phase1heartbeat.db"
    phase2db_data_file_path = "/ifs/copy_svc/phase2heartbeat.db"
    _logger = LoggerFactory().create('HeartBeatDb')

    def __init__(self, heartbeat_type):
        assert \
            os.path.exists(os.path.dirname(HeartBeatDb.phase1db_data_file_path)),\
            "Directory does not exist '{0}'".format(os.path.dirname(HeartBeatDb.phase1db_data_file_path))

        assert \
            os.path.exists(os.path.dirname(HeartBeatDb.phase2db_data_file_path)), \
            "Directory does not exist '{0}'".format(os.path.dirname(HeartBeatDb.phase1db_data_file_path))

        assert heartbeat_type == 'phase1' or heartbeat_type == 'phase2', "Unexpected heartbeat type"
        self._heartbeat_type = heartbeat_type

        command = "CREATE TABLE IF NOT EXISTS heartbeat " \
                  "(node TEXT NOT NULL, " \
                  "pid INTEGER NOT NULL, " \
                  "last_heartbeat timestamp NOT NULL, " \
                  " PRIMARY KEY (node, pid))"

        if heartbeat_type == 'phase1':
            SqliteFacade.execute_command(HeartBeatDb.phase1db_data_file_path, command)
        else:
            SqliteFacade.execute_command(HeartBeatDb.phase2db_data_file_path, command)

    def write_heartbeat(self):
        """
        :return:
        """

        sql_insert_query = 'INSERT OR REPLACE INTO heartbeat ' \
                           '(node, pid, last_heartbeat) VALUES (?,?,?)'
        parameters = (socket.gethostname(),
                      os.getpid(),
                      datetime.datetime.now())
        if self._heartbeat_type == 'phase1':
            SqliteFacade.execute_parameterized_command(
                HeartBeatDb.phase1db_data_file_path, sql_insert_query, parameters)
        else:
            SqliteFacade.execute_parameterized_command(
                HeartBeatDb.phase2db_data_file_path, sql_insert_query, parameters)

    def get_heartbeat(self, node, pid):
        """
        :type node: str
        :type pid int
        :type heartbeat_type: str
        """

        command = 'SELECT * FROM heartbeat WHERE node = ? AND pid = ?'
        parameters = (node, pid)
        if self._heartbeat_type == 'phase1':
            qry_results = SqliteFacade.execute_parameterized_select(
                HeartBeatDb.phase1db_data_file_path, command, parameters)
        else:
            qry_results = SqliteFacade.execute_parameterized_select(
                HeartBeatDb.phase2db_data_file_path, command, parameters)

        if not qry_results:
            return

        result = qry_results["last_heartbeat"],
        return result

    def delete_heartbeat(self, node, pid):
        """
        :type node: str
        :type pid int
        """

        command = "DELETE FROM heartbeat WHERE node = ? and pid = ?"
        parameters = (node, pid)

        if self._heartbeat_type == 'phase1':
            SqliteFacade.execute_parameterized_command(HeartBeatDb.phase1db_data_file_path, command, parameters)
        else:
            SqliteFacade.execute_parameterized_command(HeartBeatDb.phase2db_data_file_path, command, parameters)

    def clear_heartbeats(self):
        """
        :return:
        """
        command = "DELETE FROM heartbeat"
        if self._heartbeat_type == 'phase1':
            SqliteFacade.execute_command(HeartBeatDb.phase1db_data_file_path, command)
        else:
            SqliteFacade.execute_command(HeartBeatDb.phase2db_data_file_path, command)