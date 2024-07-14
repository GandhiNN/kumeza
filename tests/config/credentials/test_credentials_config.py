import os
import unittest

from kumeza.config.credentials.credentials_config import CredentialsConfig
from kumeza.config.loader import ConfigLoader


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class TestConfigInstanceCredentials:
    def __init__(self):
        self.credentials = CredentialsConfig(
            username="s-imel-opsdaas-qas01",
            provider="hashicorp_vault",
            url="https://vault.vault-dev-dev.shared-services.eu-west-1.aws.pmicloud.biz:8200",
            verify_ssl=False,
            namespace="icloud",
            mount_point="static-secret",
            path="data/imel",
        )


class TestSetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestCredentialsConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = TestConfigInstanceCredentials()
        expected = base_config.credentials
        self.assertEqual(
            CredentialsConfig.marshal(self.yml_config["credentials"]),
            expected,
        )
        self.assertEqual(
            CredentialsConfig.marshal(self.json_config["credentials"]),
            expected,
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestCredentialsConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
