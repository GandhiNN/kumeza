# pylint: disable=redefined-outer-name
#
import unittest

import boto3
from moto import mock_aws

from kumeza.utils.aws.secretsmanager import SecretsManager


SECRET_NAME = "mock_secret"


class SecretsManagerTestIntegration(unittest.TestCase):
    @mock_aws
    def setUp(self):
        # Setup the mock connection
        self.secretsmanager_client = boto3.session.Session().client(
            service_name="secretsmanager", region_name="eu-west-1"
        )

    @mock_aws
    def create_test_secret(self):
        self.secretsmanager_client.create_secret(
            Name="mock_secret",
            SecretString="""{"mock_secret_key": "mock_secret_value"}""",
        )

    @mock_aws
    def test_retrieve_secret_string(self):
        # Create the mock secret
        self.create_test_secret()

        # Run the test for the function
        credentials_client = SecretsManager()
        assert credentials_client.get_secret(SECRET_NAME) == {
            "mock_secret_key": "mock_secret_value"
        }
