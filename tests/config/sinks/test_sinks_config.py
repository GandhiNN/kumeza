import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.sinks.sinks_config import SinksConfig


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


class ConfigInstanceSinks:
    def __init__(self):
        self.sinks_yaml = ConfigLoader.load(YAML_CONFIG)["sinks"]
        self.sinks_json = ConfigLoader.load(JSON_CONFIG)["sinks"]


class SinksConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = ConfigInstanceSinks()
        keys_yaml = list(base_config.sinks_yaml[0].keys())
        keys_json = list(base_config.sinks_json[0].keys())

        # Keys sameness assertion
        self.assertEqual(
            SinksConfig.marshal(self.yml_config["sinks"]).targets[0].get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            SinksConfig.marshal(self.json_config["sinks"]).targets[0].get_field_name(),
            keys_json,
        )

        # Object length assertion
        self.assertEqual(
            SinksConfig.marshal(self.yml_config["sinks"]).targets[0].get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            SinksConfig.marshal(self.json_config["sinks"]).targets[0].get_length(),
            len(keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            SinksConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())
