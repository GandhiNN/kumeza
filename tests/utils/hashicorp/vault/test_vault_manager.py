import unittest
from unittest.mock import MagicMock, patch

from kumeza.utils.hashicorp.vault.vault_manager import VaultManager


class HashicorpVaultTest(unittest.TestCase):
    @patch("kumeza.utils.hashicorp.vault.vault_manager.hvac.Client")
    def test_auth_with_approle(self, hvac):
        fake_client = MagicMock()
        fake_auth_object = {"auth": {"client_token": "awesome_token"}}
        fake_client.auth.approle.login.return_value = fake_auth_object
        hvac.return_value = fake_client

        # Test actual function
        client = VaultManager(
            vault_url="http://someaddr.com",
            namespace="somenamespace",
            ssl_verify="somecertfile",
        )
        secrets = {"role_id": "someroleid", "secret_id": "somesecretid"}
        client.approle_login(role_id=secrets["role_id"], secret_id=secrets["secret_id"])
        assert (
            client.client["auth"]["client_token"]
            == fake_auth_object["auth"]["client_token"]
        )
