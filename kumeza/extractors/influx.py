# pylint: disable=attribute-defined-outside-init
import logging

import requests  # type: ignore
from influxdb import DataFrameClient, resultset

from kumeza.connectors.influx import InfluxManager


logger = logging.getLogger(__name__)


class APIExtractor:
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
        self, username: str, password: str, query: str, db_name: str
    ) -> resultset.ResultSet:
        """Query the data into an InfluxDB result set object

        ######Args:
        - Query result data (string, optional): Query statements
        to be executed. Defaults to None.

        ######Returns:
            `resultset.ResultSet`: InfluxDB result set
        """

        ssl_flag = self.influx_mgr.ssl_flag
        verify_ssl_flag = self.influx_mgr.verify_ssl

        self.influx_client = DataFrameClient(
            host=self.influx_mgr.hostname,
            port=self.influx_mgr.port,
            username=username,
            password=password,  # pragma: allowlist-secret
            database=db_name,
            ssl=ssl_flag,
            verify_ssl=verify_ssl_flag,
        )
        return self.influx_client.query(query)


class RESTExtractor:
    """
    Class to handle connections to a influx database source via HTTP REST
    """

    def __init__(self, influx_mgr: InfluxManager):
        """
        Init connection parameters
        """
        super().__init__()
        self.influx_mgr = influx_mgr

    def create_session(self, username: str, password: str):
        """
        Create REST session with authentication
        """
        self.sess = requests.Session()
        self.sess.auth = (username, password)

    def read(self, endpoint: str, header: dict) -> requests.Response:
        """Sends a GET request"""
        url = f"{self.influx_mgr.hostname}/{endpoint}"
        return self.sess.get(url, headers=header)
