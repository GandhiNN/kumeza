import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.runtime_environment.runtime_environment_config import (
    RuntimeEnvironmentConfig,
)


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


class ConfigInstanceRuntimeEnvironment:
    def __init__(self):
        self.runtime_environment_yaml = ConfigLoader.load(YAML_CONFIG)[
            "runtime_environment"
        ]
        self.runtime_environment_json = ConfigLoader.load(JSON_CONFIG)[
            "runtime_environment"
        ]


class RuntimeEnvironmentConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = ConfigInstanceRuntimeEnvironment()
        keys_yaml = list(base_config.runtime_environment_yaml.keys())
        keys_json = list(base_config.runtime_environment_json.keys())

        # Keys sameness assertion
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(
                self.yml_config["runtime_environment"]
            ).get_field_name(),
            keys_yaml,
        )
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(
                self.json_config["runtime_environment"]
            ).get_field_name(),
            keys_json,
        )

        # Object length assertion
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(
                self.yml_config["runtime_environment"]
            ).get_length(),
            len(keys_yaml),
        )
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(
                self.json_config["runtime_environment"]
            ).get_length(),
            len(keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            RuntimeEnvironmentConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
