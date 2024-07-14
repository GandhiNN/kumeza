import os
import unittest

from kumeza.config.integration.integration_config import IntegrationConfig
from kumeza.config.loader import ConfigLoader


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class TestConfigInstanceIntegration:
    def __init__(self):
        self.integration = IntegrationConfig(
            engine="spark", driver="jdbc", fetchsize=1000, chunksize=1000000
        )


class TestSetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestIntegrationConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = TestConfigInstanceIntegration()
        expected = base_config.integration
        self.assertEqual(
            IntegrationConfig.marshal(self.yml_config["integration"]),
            expected,
        )
        self.assertEqual(
            IntegrationConfig.marshal(self.json_config["integration"]),
            expected,
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestIntegrationConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
