## TODO
import os
import pytest

from unittest.mock import MagicMock, patch

from kumeza.utils.hashicorp.vault.vault_manager import VaultManager
from kumeza.config.ingestor_config import IngestionConfig

@patch("kumeza.utils.hashicorp.vault.vault_manager.VaultManager")
def test_get_credentials_using_approle(VaultManager):
    config = {
        "url": "http://someaddr.com",
        "namespace": "some_namespace",
        "verify_ssl": False,
    }

    fake_auth_object = {"auth": {"client_token": "awesome_token"}}

    fake_client = MagicMock()
    fake_client.auth_approle.return_vaule = fake_auth_object
    
    vault = VaultManager()
    vault.create_vault_client(config)