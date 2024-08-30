import datetime
from typing import Any

import pandas as pd


class DateObject:
    def __init__(
        self,
        ts_format: str = "%Y-%m-%d %H:%M:%S",
    ):
        self.ts_format = ts_format

    def get_timestamp_with_milliseconds(self, epoch: str) -> str:
        timestamp: str = "0"
        millisecond: int = 0
        epoch_int = int(epoch)
        if len(epoch) == 13:
            millisecond = epoch_int % 1000
            timestamp = datetime.datetime.fromtimestamp(
                epoch_int / 1000, tz=datetime.timezone.utc
            ).strftime(self.ts_format)
        elif len(epoch) > 0 and len(epoch) <= 10:
            timestamp = datetime.datetime.fromtimestamp(
                epoch_int, tz=datetime.timezone.utc
            ).strftime(self.ts_format)
        return f"{timestamp}.{millisecond}"

    def get_year_diff(
        self,
        timestamp_start: str = "2015-01-01 00:00:00",
        timestamp_end: str = "",
    ) -> int:
        ts_start = datetime.datetime.strptime(timestamp_start, self.ts_format)
        ts_now = (
            datetime.datetime.strptime(timestamp_end, self.ts_format)
            if timestamp_end
            else datetime.datetime.now()
        )
        return ts_now.year - ts_start.year

    def get_timestamp_range(
        self,
        datestart: str = "",
        dateend: str = "",
        freqalias: str = "MS",
    ) -> list:
        # for freqalias -> https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
        tslist = [
            i.strftime(self.ts_format)
            for i in pd.date_range(start=datestart, end=dateend, freq=freqalias)
        ]
        return tslist

    def get_current_timestamp(self, ts_format: str = "epoch") -> Any:
        """
        Get current timestamp according to desired input format.

        Parameters
        ----------
        type = kwarg ; 'epoch' = unix epoch, 'human' = YYYY-MM-DD

        Returns
        -------
        Current timestamp in desired format
        """
        if ts_format == "epoch":
            return str(int(datetime.datetime.now().timestamp() * 1000))
        if ts_format == "date_only":
            return datetime.datetime.today().strftime("%Y-%m-%d")
        if ts_format == "date_with_clock":
            return datetime.datetime.today().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
        if ts_format == "timenow_string":
            return datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        if ts_format == "datetime_object":
            return datetime.datetime.now()
