import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.metadata.metadata_config import MetadataConfig


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class SetUp:
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class ConfigInstanceMetadata:
    def __init__(self):
        self.metadata_yaml = ConfigLoader.load(YAML_CONFIG)["metadata"]
        self.metadata_json = ConfigLoader.load(JSON_CONFIG)["metadata"]


class MetadataConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()
        self.base_config = ConfigInstanceMetadata()
        self.keys_yaml = list(self.base_config.metadata_yaml[0].keys())
        self.keys_json = list(self.base_config.metadata_json[0].keys())
        self.metadata_config_yaml = MetadataConfig.marshal(self.yml_config["metadata"])
        self.metadata_config_json = MetadataConfig.marshal(self.json_config["metadata"])

    def test(self):

        print(self.metadata_config_yaml)

        # Keys sameness assertion
        self.assertEqual(
            self.metadata_config_yaml.metadata_targets[0].get_field_name(),
            self.keys_yaml,
        )
        self.assertEqual(
            self.metadata_config_json.metadata_targets[0].get_field_name(),
            self.keys_json,
        )

        # Object length assertion
        self.assertEqual(
            self.metadata_config_yaml.metadata_targets[0].get_length(),
            len(self.keys_yaml),
        )
        self.assertEqual(
            self.metadata_config_json.metadata_targets[0].get_length(),
            len(self.keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            MetadataConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())
