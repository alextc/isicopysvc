__author__ = 'alextc'
import datetime
import logging


class DateTimeUtils(object):

    datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'

    def __init__(self):
        pass

    def get_current_utc_datetime_as_formatted_string(self):
        right_now = datetime.datetime.utcnow()
        formatted = right_now.strftime(self.datetime_format_string)
        return formatted

    def parse_datetime_from_formatted_string(self, formatted_datetime):
        return datetime.datetime.strptime(formatted_datetime, self.datetime_format_string)

    @staticmethod
    def datetime_to_formatted_string(date_time):
        return date_time.strftime(DateTimeUtils.datetime_format_string)

    @staticmethod
    def datetime_to_timestamp(date_time):
        """
        :type date_time: datetime.datetime
        """
        return int(date_time.strftime("%s"))

    def is_datetime_in_expected_format(self, datetime_formatted_string):
        try:
            formatted = datetime.datetime.strptime(datetime_formatted_string, self.datetime_format_string)
            if formatted:
                return True
        except ValueError:
            return False

        return False

    @staticmethod
    def get_total_seconds_for_timedelta(timedelta):
        """
        :type timedelta: datetime.timedelta
        :rtype: int
        """
        logging.debug("About to calculate delta_in_seconds for {0}".format(timedelta))
        delta_in_seconds = timedelta.days * 86400 + timedelta.seconds
        logging.debug("_get_total_seconds_for_time_delta is returning: {0}".format(delta_in_seconds))
        return delta_in_seconds

    @staticmethod
    def strip_microseconds(datetime_with_microseconds):
        return datetime.datetime(
                year=datetime_with_microseconds.year,
                month=datetime_with_microseconds.month,
                day=datetime_with_microseconds.day,
                hour=datetime_with_microseconds.hour,
                minute=datetime_with_microseconds.minute,
                second=datetime_with_microseconds.second)
