import os
import unittest

from kumeza.config.integration.integration_config import IntegrationConfig
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


class ConfigInstanceIntegration:
    def __init__(self):
        self.integration_yaml = ConfigLoader.load(YAML_CONFIG)["integration"]
        self.integration_json = ConfigLoader.load(JSON_CONFIG)["integration"]


class IntegrationConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = ConfigInstanceIntegration()
        keys_yaml = list(base_config.integration_yaml.keys())
        keys_json = list(base_config.integration_json.keys())

        # Keys sameness assertion
        self.assertEqual(
            IntegrationConfig.marshal(self.yml_config["integration"]).get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            IntegrationConfig.marshal(self.json_config["integration"]).get_field_name(),
            keys_json,
        )

        # Object length assertion
        self.assertEqual(
            IntegrationConfig.marshal(self.yml_config["integration"]).get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            IntegrationConfig.marshal(self.json_config["integration"]).get_length(),
            len(keys_json),
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            IntegrationConfigTest,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
