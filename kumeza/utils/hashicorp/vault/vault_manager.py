import hvac

from typing import Any
from kumeza.config.ingestor_config import IngestionConfig

class VaultManager:

    def __init__(self, ingestion_config=IngestionConfig):
        self.namespace: str = ingestion_config.credentials.namespace
        self.env: str = ingestion_config.runtime_environment.env
        self.vault_url: str = ingestion_config.credentials.url
        self.verify_ssl: bool = ingestion_config.credentials.verify_ssl
        self.client: hvac.Client = None

    def setup_client(self):
        self.client = hvac.Client(
            url = self.vault_url,
            namespace=self.namespace,
            verify=self.verify_ssl
        )

    