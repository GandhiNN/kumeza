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


class TestConfigInstanceRuntimeEnvironment:
    def __init__(self):
        self.runtime_environment = RuntimeEnvironmentConfig(
            id="icloud", provider="aws", service="glue", region="eu-west-1", env="dev"
        )


class TestSetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestRuntimeEnvironmentConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = TestConfigInstanceRuntimeEnvironment()
        expected = base_config.runtime_environment
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(self.yml_config["runtime_environment"]),
            expected,
        )
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(self.json_config["runtime_environment"]),
            expected,
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestRuntimeEnvironmentConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
