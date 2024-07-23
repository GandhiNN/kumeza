import os

from kumeza.utils.aws.secretsmanager import SecretsManager
from kumeza.utils.hashicorp.vault import VaultManager

# Constants
ABS_PATH = os.path.dirname(__file__)
CERT_FILE = os.path.join(ABS_PATH, "kumeza", "cert", "pki_bundle.crt")
AWS_PROFILE_NAME = "291751643970_tlz_developer"
SECRET_PATH = "secret/icloud-ingestion/vault/dev/credential"
NAMESPACE = "icloud-dev"
VAULT_URL = "https://dev.vault-dev-dev.eu-west-1.aws.private-pmideep.biz:8200"

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
secrets = secretsmanager.get_secret(SECRET_PATH)
print(secrets)

# Logic Vaultmanager
vaultmanager = VaultManager()
vaultmanager.create_vault_client(vault_config)
print(vaultmanager)
# approle login
vaultmanager.approle_login(role_id=secrets['role_id'], secret_id=secrets['secret_id'])
# get secret
