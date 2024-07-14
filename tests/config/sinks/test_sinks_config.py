import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.sinks.sinks_config import Sinks, SinksConfig


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class TestConfigInstanceSinks:
    def __init__(self):
        self.sinks = SinksConfig(
            [
                Sinks(
                    id="daas_raw_bucket",
                    target="s3",
                    file_format="parquet",
                    path="daas-s3-raw-dev",
                ),
                Sinks(
                    id="enterprise_landing_raw_bucket",
                    target="s3",
                    file_format="parquet",
                    path="enterprise-landing-raw-dev",
                ),
                Sinks(
                    id="enterprise_landing_schema_bucket",
                    target="s3",
                    file_format="json",
                    path="enterprise-landing-schema-raw-dev",
                ),
            ]
        )


class TestSetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestSinksConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = TestConfigInstanceSinks()
        expected = base_config.sinks
        self.assertEqual(
            SinksConfig.marshal(self.yml_config["sinks"]),
            expected,
        )
        self.assertEqual(
            SinksConfig.marshal(self.json_config["sinks"]),
            expected,
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestSinksConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
