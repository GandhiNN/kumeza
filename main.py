import os

from kumeza.utils.aws.secretsmanager.secretsmanager import SecretsManager
from kumeza.utils.hashicorp.vault.vault_manager import VaultManager

# Constants
ABS_PATH = os.path.dirname(__file__)
CERT_FILE = os.path.join(ABS_PATH, "kumeza", "shared_lib", "cert", "pki_bundle.crt")
SECRET_NAME = "hcv-icloud-approle-dev"
NAMESPACE = "icloud-dev"
VAULT_URL = (
    "https://vault.vault-dev-dev.shared-services.eu-west-1.aws.pmicloud.biz:8200"
)
MOUNT_POINT = "static-secret"
PATH = "data/daas/imel_sandbox"

# SecretsManager Config
vault_config = {"url": VAULT_URL, "namespace": NAMESPACE, "verify_ssl": CERT_FILE}

# Retrieve secrets from Secrets Manager
secretsmanager = SecretsManager()
secrets = secretsmanager.get_secret(SECRET_NAME)
print(secrets)

# Retrieve credentials from Hashicorp Vault
vaultmanager = VaultManager(
    vault_url=VAULT_URL, namespace=NAMESPACE, ssl_verify=CERT_FILE
)
vaultmanager.set_client_with_approle_auth(
    role_id=secrets["role_id"], secret_id=secrets["secret_id"]
)
creds = vaultmanager.get_credentials(PATH, MOUNT_POINT)
print(creds)
