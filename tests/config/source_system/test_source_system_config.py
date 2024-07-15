import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.source_system.source_system_config import SourceSystemConfig


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


class ConfigInstanceSourceSystem:
    def __init__(self):
        self.source_system_yaml = ConfigLoader.load(YAML_CONFIG)["source_system"]
        self.source_system_json = ConfigLoader.load(JSON_CONFIG)["source_system"]


class SourceSystemConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = ConfigInstanceSourceSystem()
        keys_yaml = list(base_config.source_system_yaml.keys())
        keys_json = list(base_config.source_system_json.keys())

        # Keys sameness assertion
        self.assertEqual(
            SourceSystemConfig.marshal(
                self.yml_config["source_system"]
            ).get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            SourceSystemConfig.marshal(
                self.json_config["source_system"]
            ).get_field_name(),
            keys_json,
        )
        # expected = base_config.source_system
        self.assertEqual(
            SourceSystemConfig.marshal(self.yml_config["source_system"]).get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            SourceSystemConfig.marshal(self.json_config["source_system"]).get_length(),
            len(keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            SourceSystemConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
