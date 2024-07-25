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

    def test(self):
        base_config = ConfigInstanceMetadata()
        keys_yaml = list(base_config.metadata_yaml.keys())
        keys_json = list(base_config.metadata_json.keys())

        # Keys sameness assertion
        self.assertEqual(
            MetadataConfig.marshal(self.yml_config["metadata"]).get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            MetadataConfig.marshal(self.json_config["metadata"]).get_field_name(),
            keys_json,
        )

        # Object length assertion
        self.assertEqual(
            MetadataConfig.marshal(self.yml_config["metadata"]).get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            MetadataConfig.marshal(self.json_config["metadata"]).get_length(),
            len(keys_json),
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
