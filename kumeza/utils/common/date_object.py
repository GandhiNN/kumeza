import datetime


class DateObject:
    def __init__(
        self,
        ts_format: str = "%Y-%m-%d %H:%M:%S",
    ):
        self.ts_format = ts_format

    def get_human_readable_timestamp(self, epoch: str) -> str:
        timestamp: str = "0"
        epoch_int = int(epoch)
        if len(epoch) == 13:
            timestamp = datetime.datetime.fromtimestamp(
                epoch_int / 1000, tz=datetime.timezone.utc
            ).strftime(self.ts_format)
        elif len(epoch) > 0 and len(epoch) <= 10:
            timestamp = datetime.datetime.fromtimestamp(
                epoch_int, tz=datetime.timezone.utc
            ).strftime(self.ts_format)
        return f"{timestamp}"

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

    def get_timestamp_range(self, start: str, end: str, num_intervals: int) -> list:
        """
        Given a date range, break it up into N contiguous sub-intervals.
        Argument to `start` and `end` must adhere to "YYYY-MM-DD HH:MM:ss" format
        """
        sub_int: list = []
        ts_start = datetime.datetime.strptime(start, self.ts_format)
        ts_end = datetime.datetime.strptime(end, self.ts_format)
        diff = (ts_end - ts_start) / num_intervals
        for i in range(num_intervals):
            subdate = (ts_start + (diff * i)).strftime(self.ts_format)
            sub_int.append(subdate)
        sub_int.append(end)  # inclusive of end date
        return sub_int

    def get_timestamp_n_days_ago(self, ts: str, n: int) -> str:
        date_format = "%Y-%m-%d"
        date_n = datetime.datetime.strptime(ts, date_format) - datetime.timedelta(n)
        return date_n.strftime(date_format)

    def get_timestamp_n_days_from(self, ts: str, n: int) -> str:
        date_format = "%Y-%m-%d"
        date_n = datetime.datetime.strptime(ts, date_format) + datetime.timedelta(n)
        return date_n.strftime(date_format)

    def get_timestamp_as_str(self, ts_format: str = "epoch") -> str:
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
        if ts_format == "date_filename":
            return datetime.datetime.today().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
        if ts_format == "timenow_string":
            return datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        if ts_format == "datetime_object":
            return datetime.datetime.now()
