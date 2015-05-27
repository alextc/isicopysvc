__author__ = 'alextc'
import datetime

class DateTimeWrapper():
    _datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'

    def get_current_utc_datetime_as_formatted_string(self):
        right_now = datetime.datetime.utcnow()
        formatted = right_now.strftime(self._datetime_format_string)
        return  formatted


    def parse_datetime_from_formatted_string(self, formatted_datetime):
        return datetime.datetime.strptime(formatted_datetime, self._datetime_format_string)

    def is_datetime_in_expected_format(self, datetime_formatted_string):
        try:
            formatted = datetime.datetime.strptime(datetime_formatted_string, self._datetime_format_string)
            if formatted:
                return  True
        except ValueError:
            return  False

        return False
