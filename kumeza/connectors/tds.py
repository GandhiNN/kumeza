import logging


logger = logging.getLogger(__name__)


class TDSManager:

    def __init__(self, hostname: str, port: int, db_instance: str):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance

    def get_connection_string(self) -> str:
        logger.info("Getting connection string")
        logger.info("Connection string: %s:%s\\%s", self.hostname, self.port, self.db_instance)
        return f"{self.hostname}:{self.port}\\{self.db_instance}"
