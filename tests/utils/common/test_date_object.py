import datetime
import unittest

from kumeza.utils.common.date_object import DateObject


class DateObjectTest(unittest.TestCase):

    def setUp(self):
        self.dateobject = DateObject()

    def test_get_human_readable_timestamp(self):
        test_epoch_10_digits = "1724664819"
        result_10_digits_utc = self.dateobject.get_human_readable_timestamp(
            test_epoch_10_digits
        )
        assert result_10_digits_utc == "2024-08-26 09:33:39"
        test_epoch_13_digits = "1724664819123"
        result_13_digits_utc = self.dateobject.get_human_readable_timestamp(
            test_epoch_13_digits
        )
        assert result_13_digits_utc == "2024-08-26 09:33:39"

    def test_get_year_diff(self):
        ts_start = "2015-01-01 00:00:00"
        ts_end = "2024-08-26 00:00:00"
        assert self.dateobject.get_year_diff(ts_start, ts_end) == 9

    def test_get_timestamp_range_even_split(self):
        ts_start = "2020-01-01 00:00:00"
        ts_end = "2024-01-01 00:00:00"
        num_intervals = 4
        tslist = [
            "2020-01-01 00:00:00",
            "2020-12-31 06:00:00",
            "2021-12-31 12:00:00",
            "2022-12-31 18:00:00",
            "2024-01-01 00:00:00",
        ]
        assert (
            self.dateobject.get_timestamp_range(
                start=ts_start, end=ts_end, num_intervals=num_intervals
            )
            == tslist
        )

    def test_get_current_timestamp(self):
        # Check if epoch is producing length of 13 chars
        assert len(self.dateobject.get_current_timestamp(ts_format="epoch")) == 13

        # Check if date_only format adheres to "%Y-%m-%d" format
        assert datetime.datetime.strptime(
            self.dateobject.get_current_timestamp(ts_format="date_only"), "%Y-%m-%d"
        )
        assert len(self.dateobject.get_current_timestamp(ts_format="date_only")) == 10

        # Check if date_with_clock format adheres to "%Y-%m-%d-%H-%M-%S-%f" format
        assert datetime.datetime.strptime(
            self.dateobject.get_current_timestamp(ts_format="date_filename"),
            "%Y-%m-%d-%H-%M-%S-%f",
        )
        assert (
            len(self.dateobject.get_current_timestamp(ts_format="date_filename")) == 23
        )

        # Check if timenow_string format adheres to "%Y-%m-%d %H:%M:%S" format
        assert datetime.datetime.strptime(
            self.dateobject.get_current_timestamp(ts_format="timenow_string"),
            "%Y-%m-%d %H:%M:%S",
        )
        assert (
            len(self.dateobject.get_current_timestamp(ts_format="timenow_string")) == 19
        )

        # Test if raw datetime object is returned
        assert isinstance(
            self.dateobject.get_current_timestamp(ts_format="datetime_object"),
            datetime.datetime,
        )
