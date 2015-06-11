__author__ = 'alextc'

import unittest
import datetime
from common.datetimeutils import DateTimeUtils

class WriteLockDbTests(unittest.TestCase):
    _datetime_format_string = '%Y, %m, %d, %H, %M, %S, %f'

    def test_must_format_current_date(self):
        wrapper = DateTimeUtils()
        formatted_utc_datetime_string = \
            wrapper.get_current_utc_datetime_as_formatted_string()
        datetime_parsed_from_formatted_string = \
            wrapper.parse_datetime_from_formatted_string(formatted_utc_datetime_string)

        self.assertEquals(
            datetime_parsed_from_formatted_string,
            datetime.datetime.strptime(formatted_utc_datetime_string, self._datetime_format_string))

    def test_must_return_false_when_datetime_is_not_in_expected_format(self):
        wrapper = DateTimeUtils()
        # Second number is expected to be months so should be 1-12
        self.assertFalse(wrapper.is_datetime_in_expected_format("2015, 22, 10, 12, 12, 12, 12"))

if __name__ == '__main__':
    unittest.main()
