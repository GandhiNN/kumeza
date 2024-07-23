# pylint: disable=redefined-outer-name
#
import boto3
import pytest
from moto import mock_aws

from kumeza.utils.aws.secretsmanager.secretsmanager import SecretsManager


SECRET_NAME = "mock_secret"


@pytest.fixture
def create_mocked_secret_manager_connection():
    with mock_aws():
        yield boto3.session.Session().client(
            service_name="secretsmanager", region_name="eu-west-1"
        )


@pytest.fixture
def create_test_secret(create_mocked_secret_manager_connection):
    create_mocked_secret_manager_connection.create_secret(
        Name="mock_secret",
        SecretString="""{"mock_secret_key": "mock_secret_value"}""",
    )


def test_retrieve_secret_string(
    monkeypatch, create_mocked_secret_manager_connection, create_test_secret
):
    def get_mocked_secret_manager_connection(*args, **kwargs):
        return create_mocked_secret_manager_connection

    monkeypatch.setattr(
        SecretsManager, "_create_boto_client", get_mocked_secret_manager_connection
    )

    credentials_client = SecretsManager()

    assert credentials_client.get_secret(SECRET_NAME) == {
        "mock_secret_key": "mock_secret_value"
    }
