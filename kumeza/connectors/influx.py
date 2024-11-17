import logging


logger = logging.getLogger(__name__)


class InfluxManager:
    def __init__(
        self,
        hostname: str,
        port: int,
        db_instance: str,
        authentication_type: str,
        ssl_flag: bool = True,
        verify_ssl: bool = False,
    ):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.authentication_type = authentication_type
        self.ssl_flag = ssl_flag
        self.verify_ssl = verify_ssl
