# pylint: disable=attribute-defined-outside-init
import logging

from influxdb import DataFrameClient, resultset

from kumeza.connectors.influx import InfluxManager


logger = logging.getLogger(__name__)


class InfluxExtractor:
    """
    Class to handle connections to a influx database source
    """

    def __init__(self, influx_mgr: InfluxManager):
        """
        Init connection parameters
        """
        super().__init__()
        self.influx_mgr = influx_mgr

    def read(
        self, db_name: str, query: str, username: str, password: str
    ) -> resultset.ResultSet:
        """Query the data into an InfluxDB result set object

        ######Args:
        - Query result data (string, optional): Query statements
        to be executed. Defaults to None.

        ######Returns:
            `resultset.ResultSet`: InfluxDB result set
        """
        del db_name  # unused

        ssl_flag = self.influx_mgr.ssl_flag
        verify_ssl_flag = self.influx_mgr.verify_ssl

        self.influx_client = DataFrameClient(
            host=self.influx_mgr.hostname,
            port=self.influx_mgr.port,
            username=username,
            password=password,
            database=self.influx_mgr.db_instance,
            ssl=ssl_flag,
            verify_ssl=verify_ssl_flag,
        )
        return self.influx_client.query(query)
