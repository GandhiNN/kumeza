import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.metadata.metadata_config import MetadataConfig


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class TestConfigInstanceMetadata:
    def __init__(self):
        self.metadata = MetadataConfig(
            sink_type="dynamodb",
            table_name="daas-imel-ingestion-status-dev",
        )


class TestSetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestMetadataConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = TestConfigInstanceMetadata()
        expected = base_config.metadata
        self.assertEqual(
            MetadataConfig.marshal(self.yml_config["metadata"]),
            expected,
        )
        self.assertEqual(
            MetadataConfig.marshal(self.json_config["metadata"]),
            expected,
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestMetadataConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
