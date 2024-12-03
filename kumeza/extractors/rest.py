# pylint: disable=attribute-defined-outside-init
import logging
import sys
import textwrap
from typing import Tuple

import requests  # type: ignore
from requests.auth import HTTPBasicAuth  # type: ignore
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from kumeza.connectors.influx import InfluxManager


logger = logging.getLogger(__name__)

disable_warnings(InsecureRequestWarning)


def print_roundtrip(response, *args, **kwargs):
    def format_headers(d: dict):
        return "\n".join(f"{k}: {v}" for k, v in d.items())

    print(
        textwrap.dedent(
            """
        ---------------- request ----------------
        {req.method} {req.url}
        {reqhdrs}

        {req.body}
        ---------------- response ----------------
        {res.status_code} {res.reason} {res.url}
        {reshdrs}

        {res.text}
    """
        ).format(
            req=response.request,
            res=response,
            reqhdrs=format_headers(response.request.headers),
            reshdrs=format_headers(response.headers),
        )
    )


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
        self.sess.auth = HTTPBasicAuth(username, password)

    def close_session(self):
        self.sess.close()

    def read(
        self,
        base_url: str,
        header: dict,
        params: dict,
        verbose: bool = False,
        stream: bool = True,
    ) -> Tuple[requests.Response, int]:
        """Sends a GET request"""
        if verbose:
            return self.sess.get(
                url=base_url,
                headers=header,
                params=params,
                verify=False,
                stream=stream,
                hooks={"response": print_roundtrip},
            )
        res = self.sess.get(
            url=base_url, headers=header, params=params, verify=False, stream=stream
        )
        return res, sys.getsizeof(res)
