# pylint: disable=attribute-defined-outside-init
import logging

import requests  # type: ignore

from kumeza.connectors.influx import InfluxManager


logger = logging.getLogger(__name__)


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

    def read(
        self, base_url: str, header: dict, params: dict, payload: dict
    ) -> requests.Response:
        """Sends a GET request"""
        return self.sess.get(url=base_url, headers=header, params=params, data=payload)
