import hvac

from kumeza.config.ingestor_config import IngestionConfig


class VaultManager:

    def __init__(self, ingestion_config=IngestionConfig):
        self.client = None

    def create_vault_client(self, config: dict):
        self.client = hvac.Client(
            url=config.url, namespace=config.namespace, verify=config.verify_ssl
        )

    def approle_login(self, role_id: str, secret_id: str):
        self.client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    def get_secret(self):
        pass
