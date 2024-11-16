import logging


logger = logging.getLogger(__name__)


class InfluxManager:
    def __init__(
        self,
        url: str,
        port: int,
        db_instance: str,
        ssl_flag: bool = True,
        verify_ssl: bool = False,
    ):
        self.url = url
        self.port = port
        self.db_instance = db_instance
        self.ssl_flag = ssl_flag
        self.verify_ssl = verify_ssl
