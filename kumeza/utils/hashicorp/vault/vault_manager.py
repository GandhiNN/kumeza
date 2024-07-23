import hvac


class VaultManager:

    def __init__(self, vault_url, namespace, ssl_verify=False):

        self.url = vault_url
        self.namespace = namespace
        self.ssl_verify = ssl_verify
        self.client = hvac.Client(
            url=self.url, namespace=self.namespace, verify=self.ssl_verify
        )

    def approle_login(self, role_id, secret_id):
        self.client = self.client.auth.approle.login(
            role_id=role_id, secret_id=secret_id
        )

    def get_creds(self, path, mount_point):
        return self.client.client.secrets.kv.v1.read_secret(
            path=path, mount_point=mount_point
        )
