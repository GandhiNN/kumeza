import datetime
import unittest

from kumeza.utils.common.date_object import DateObject


# raise unittest.SkipTest("##TODO")


class DateObjectTest(unittest.TestCase):

    def setUp(self):
        self.dateobject = DateObject()

    def test_get_timestamp_milliseconds(self):
        test_epoch_10_digits = "1724664819"
        result_10_digits = self.dateobject.get_timestamp_with_milliseconds(
            test_epoch_10_digits
        )
        assert result_10_digits == "2024-08-26 16:33:39.0"
        test_epoch_13_digits = "1724664819123"
        result_13_digits = self.dateobject.get_timestamp_with_milliseconds(
            test_epoch_13_digits
        )
        assert result_13_digits == "2024-08-26 16:33:39.123"

    def test_get_year_diff(self):
        ts_start = "2015-01-01 00:00:00"
        ts_end = "2024-08-26 00:00:00"
        assert self.dateobject.get_year_diff(ts_start, ts_end) == 9

    def test_get_timestamp_range(self):
        ts_start = "2024-01-01 00:00:00"
        ts_end = "2024-08-26 00:00:00"
        freqalias = "MS"
        tslist = [
            "2024-01-01 00:00:00",
            "2024-02-01 00:00:00",
            "2024-03-01 00:00:00",
            "2024-04-01 00:00:00",
            "2024-05-01 00:00:00",
            "2024-06-01 00:00:00",
            "2024-07-01 00:00:00",
            "2024-08-01 00:00:00",
        ]
        assert (
            self.dateobject.get_timestamp_range(
                datestart=ts_start, dateend=ts_end, freqalias=freqalias
            )
            == tslist
        )

    def test_get_current_timestamp(self):
        # Check if epoch is producing length of 13 chars
        assert len(self.dateobject.get_current_timestamp(ts_format="epoch")) == 13

        # Check if schemafile format adheres to "%Y-%m-%d" format
        assert datetime.datetime.strptime(
            self.dateobject.get_current_timestamp(ts_format="schemafile"), "%Y-%m-%d"
        )
        assert len(self.dateobject.get_current_timestamp(ts_format="schemafile")) == 10

        # Check if schemafile format adheres to "%Y-%m-%d-%H-%M-%S-%f" format
        assert datetime.datetime.strptime(
            self.dateobject.get_current_timestamp(ts_format="rawfile"),
            "%Y-%m-%d-%H-%M-%S-%f",
        )
        assert len(self.dateobject.get_current_timestamp(ts_format="rawfile")) == 23

        # Check if schemafile format adheres to "%Y-%m-%d %H:%M:%S" format
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
