import pytest

from kumeza.config.ingestor_config import IngestionConfig

@pytest.mark.skip(reason="no way of currently testing this")
class MSSQL:
    def __init__(self):
        self.authentication_type = None
        self.database_instance = None
        self.hostname = None
        self.username = None
        self.password = None
        self.port = None
        self.driver = None

    
    def build_connection(self, config: IngestionConfig):
        self.authentication_type = config.source_system.authentication_type
        self.database_instance = config.source_system.database_instance
        self.hostname = config.source_system.hostname
        self.username = config.credentials.username
        self.password = config.credentials or "placeholder"
        self.port = config.source_system.port
        self.driver = config.integration.driver or "ODBC Driver 18 for SQL Server"
