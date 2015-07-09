__author__ = 'alextc'
import sqlite3
from log.loggerfactory import LoggerFactory
from datetime import datetime
from common.datetimeutils import DateTimeUtils


class SqliteFacade(object):

    _logger = LoggerFactory().create('SqliteFacade')

    def __init__(self):
        pass

    @staticmethod
    def execute_parameterized_select(db_path, select_statement, parameters):
        SqliteFacade._log_sql_command(select_statement, parameters, db_path)

        try:
            with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                connection.row_factory = sqlite3.Row
                cursor = connection.cursor()
                cursor.execute(select_statement, parameters)
                return cursor.fetchall()
        except sqlite3.Error as e:
            SqliteFacade._logger.debug(e.message)
            raise

    @staticmethod
    def execute_select(db_path, select_statement):
        SqliteFacade._log_sql_command(select_statement, None, db_path)

        try:
            with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                connection.row_factory = sqlite3.Row
                cursor = connection.cursor()
                cursor.execute(select_statement)
                return cursor.fetchall()
        except sqlite3.Error as e:
            SqliteFacade._logger.debug(e.message)
            raise

    @staticmethod
    def execute_parameterized_command(db_path, command_statement, parameters):
        SqliteFacade._log_sql_command(command_statement, parameters, db_path)

        try:

            with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                connection.row_factory = sqlite3.Row
                cursor = connection.cursor()
                cursor.execute(command_statement, parameters)
        except sqlite3.Error as e:
            SqliteFacade._logger.debug(e.message)
            raise

    @staticmethod
    def execute_command(db_path, command_statement):
        SqliteFacade._log_sql_command(command_statement, "", db_path)

        try:

            with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as connection:
                connection.row_factory = sqlite3.Row
                cursor = connection.cursor()
                cursor.execute(command_statement)
        except sqlite3.Error as e:
            SqliteFacade._logger.debug(e.message)
            raise

    @staticmethod
    def _log_sql_command(select_statement, parameters, db_path):
        if parameters:
            SqliteFacade._logger.debug(
                "About to execute\n{0}\nwith parameters:{1}\nagainst:{2}".format(
                    select_statement,
                    SqliteFacade._parameters_tostring(parameters),
                    db_path))
        else:
            SqliteFacade._logger.debug(
                "About to execute\n{0}\nagainst:{1}".format(
                    select_statement,
                    db_path))

    @staticmethod
    def _parameters_tostring(parameters):
        if parameters[0] is datetime:
            parameters_as_string = ','.join([DateTimeUtils.datetime_to_formatted_string(p) for p in parameters])
        else:
            parameters_as_string = ','.join([str(p) for p in parameters])
        return parameters_as_string