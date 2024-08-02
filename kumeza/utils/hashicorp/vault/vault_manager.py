import hvac

from kumeza import StaticFiles


class VaultManager:

    def __init__(self, vault_url, namespace, ssl_verify=StaticFiles.PKI_CERT):

        self.url = vault_url
        self.namespace = namespace
        self.ssl_verify = ssl_verify
        self.client = hvac.Client(
            url=self.url, namespace=self.namespace, verify=self.ssl_verify
        )

    def set_client_with_approle_auth(self, role_id, secret_id):
        self.client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    def get_credentials(self, path, mount_point):
        return self.client.secrets.kv.v1.read_secret(
            path=path, mount_point=mount_point
        )["data"]["data"]
