import os

from kumeza.utils.aws.secretsmanager import SecretsManager
from kumeza.utils.hashicorp.vault import VaultManager

# Constants
ABS_PATH = os.path.dirname(__file__)
CERT_FILE = os.path.join(ABS_PATH, "kumeza", "cert", "pki_bundle.crt")
AWS_PROFILE_NAME = "291751643970_tlz_developer"
SECRETS_MANAGER_PATH = "secret/icloud-ingestion/vault/dev/credential"
NAMESPACE = "icloud-dev"
# VAULT_URL = "https://dev.vault-dev-dev.eu-west-1.aws.private-pmideep.biz:8200"
VAULT_URL = "https://vault.vault-dev-dev.shared-services.eu-west-1.aws.pmicloud.biz:8200"
SECRET_MOUNT_POINT = 'static-secret'
SECRET_PATH = 'data/daas/imel'

# SecretsManager Config
vault_config = {
    'url': VAULT_URL,
    'namespace': NAMESPACE,
    'verify_ssl': CERT_FILE
}
print(vault_config)

# print(vault_config['url'])
# Logic SecretsManager
secretsmanager = SecretsManager()
secrets = secretsmanager.get_secret(SECRETS_MANAGER_PATH)
print(secrets)

# Logic Vaultmanager
vaultmanager = VaultManager(vault_url=VAULT_URL, namespace=NAMESPACE, ssl_verify=CERT_FILE)
vaultmanager.set_client_with_approle_auth(role_id=secrets['role_id'], secret_id=secrets['secret_id'])
# print(client)
# print(client.auth)
# print(client.token)
print(vaultmanager.client.auth.token)

creds = vaultmanager.get_credentials(SECRET_PATH, SECRET_MOUNT_POINT)
print(creds)