import os
import unittest

from kumeza.config.credentials.credentials_config import CredentialsConfig
from kumeza.config.loader import ConfigLoader


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class SetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class ConfigInstanceCredentials:
    def __init__(self):
        self.credentials_plain_yaml = ConfigLoader.load(YAML_CONFIG)["credentials"]
        self.credentials_plain_json = ConfigLoader.load(JSON_CONFIG)["credentials"]


class CredentialsConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        # Set base config object
        base_config = ConfigInstanceCredentials()
        keys_yaml = list(base_config.credentials_plain_yaml.keys())
        keys_json = list(base_config.credentials_plain_json.keys())

        # Keys sameness assertion
        self.assertEqual(
            CredentialsConfig.marshal(self.yml_config["credentials"]).get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            CredentialsConfig.marshal(self.json_config["credentials"]).get_field_name(),
            keys_json,
        )

        # Object length assertion
        self.assertEqual(
            CredentialsConfig.marshal(self.yml_config["credentials"]).get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            CredentialsConfig.marshal(self.json_config["credentials"]).get_length(),
            len(keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            CredentialsConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
