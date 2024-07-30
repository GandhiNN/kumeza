import os

from dotenv import load_dotenv

from kumeza.config.ingestor_config import IngestionConfig


# Load environment variables
load_dotenv()


class MSSQL:
    def __init__(self):
        self.authentication_type = None
        self.database_instance = None
        self.hostname = None
        self.username = None
        self.password = None
        self.port = None
        self.driver = None

    def create_connection(self, config: IngestionConfig):
        self.authentication_type = config.source_system.authentication_type
        self.database_instance = config.source_system.database_instance
        self.hostname = config.source_system.hostname
        self.username = config.credentials.username or os.getenv("DB_USERNAME")
        self.password = config.credentials or os.getenv("DB_PASSWORD")
        self.port = config.source_system.port
        self.driver = config.integration.driver or "FreeTDS"
