import os
import unittest

from kumeza.config.integration.rest import RestConfig
from kumeza.config.loader import ConfigLoader


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


class ConfigInstanceREST:
    def __init__(self):
        self.rest_yaml = ConfigLoader.load(YAML_CONFIG)["rest"]
        self.rest_json = ConfigLoader.load(JSON_CONFIG)["rest"]


class RESTConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = ConfigInstanceREST()
        keys_yaml = list(base_config.rest_yaml.keys())
        keys_json = list(base_config.rest_json.keys())

        # Keys sameness assertion
        self.assertEqual(
            RestConfig.marshal(self.yml_config["rest"]).get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            RestConfig.marshal(self.json_config["rest"]).get_field_name(),
            keys_json,
        )

        # Object length assertion
        self.assertEqual(
            RestConfig.marshal(self.yml_config["rest"]).get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            RestConfig.marshal(self.json_config["rest"]).get_length(),
            len(keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            RESTConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())
