import json
import unittest
from unittest.mock import MagicMock, patch

from kumeza.utils.hashicorp.vault.vault_manager import VaultManager


class HashicorpVaultTest(unittest.TestCase):

    @patch("kumeza.utils.hashicorp.vault.vault_manager.hvac.Client")
    def test_auth_client_with_approle(self, hvac):
        client = MagicMock()
        fake_auth_object = {"auth": {"client_token": "awesome_token"}}
        client.auth.token = "awesome_token"
        hvac.return_value = client

        # Test actual function
        vm = VaultManager(
            vault_url="http://someaddr.com",
            namespace="somenamespace",
            ssl_verify="somecertfile",
        )
        vm.set_client_with_approle_auth(role_id="someroleid", secret_id="somesecretid")
        assert vm.client.auth.token == fake_auth_object["auth"]["client_token"]

    @patch("kumeza.utils.hashicorp.vault.vault_manager.hvac.Client")
    def test_get_credentials(self, hvac):
        client = MagicMock()
        cred = {"password": "somepassword", "username": "someusername"}
        client.secrets.kv.v1.read_secret.return_value = {
            "data": {"data": json.dumps(cred)}
        }
        hvac.return_value = client

        # Test actual function
        vm = VaultManager(
            vault_url="http://someaddr.com",
            namespace="somenamespace",
            ssl_verify="somecertfile",
        )
        creds = vm.get_credentials("data/daas/imel", "static-secret")
        assert json.loads(creds) == cred
