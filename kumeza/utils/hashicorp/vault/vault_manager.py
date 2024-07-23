import hvac

from kumeza.config.ingestor_config import IngestionConfig


class VaultManager:

    def __init__(self):
        self.client = None

    def create_vault_client(self, config: dict):
        self.client = hvac.Client(
            url=config['url'], namespace=config['namespace'], verify=config['verify_ssl']
        )

    def approle_login(self, role_id: str, secret_id: str):
        self.client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    def get_secret(self, path: str, mount_point: str) -> dict:
        return self.client.secrets.kv.v1.read_secret(
            path=path, mount_point=mount_point
        )
