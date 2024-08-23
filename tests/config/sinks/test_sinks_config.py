import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.sinks.sinks_config import Sinks, SinksConfig, SinkTargets


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
        self.base_config = ConfigInstanceSinks()
        self.keys_yaml = list(self.base_config.sinks_yaml[0].keys())
        self.keys_json = list(self.base_config.sinks_json[0].keys())
        self.sinks_config_yaml = SinksConfig.marshal(self.yml_config["sinks"])
        self.sinks_config_json = SinksConfig.marshal(self.json_config["sinks"])

    def test_key_sameness(self):
        # Key sameness assertion
        self.assertEqual(
            self.sinks_config_yaml.sink_type[0].get_field_name(),
            self.keys_yaml,
        )
        self.assertEqual(
            self.sinks_config_json.sink_type[0].get_field_name(),
            self.keys_json,
        )

    def test_object_length(self):
        # Object length assertion
        self.assertEqual(
            self.sinks_config_yaml.sink_type[0].get_length(),
            len(self.keys_yaml),
        )
        self.assertEqual(
            self.sinks_config_json.sink_type[0].get_length(),
            len(self.keys_json),
        )

    def test_get_sinks(self):
        # Set expected output
        expected_raw = Sinks(
            sink_type="raw",
            sink_targets=[
                SinkTargets(
                    id="daas_raw_bucket",
                    target="s3",
                    file_format="parquet",
                    path="daas-s3-raw-dev",
                ),
                SinkTargets(
                    id="enterprise_landing_raw_bucket",
                    target="s3",
                    file_format="parquet",
                    path="enterprise-landing-raw-dev",
                ),
            ],
        )
        expected_schema = Sinks(
            sink_type="schema",
            sink_targets=[
                SinkTargets(
                    id="enterprise_landing_schema_bucket",
                    target="s3",
                    file_format="json",
                    path="enterprise-landing-schema-raw-dev",
                )
            ],
        )
        # Test raw sinks
        assert self.sinks_config_yaml.get_sink("raw") == expected_raw
        assert self.sinks_config_json.get_sink("raw") == expected_raw

        # Test schema sinks
        assert self.sinks_config_yaml.get_sink("schema") == expected_schema
        assert self.sinks_config_json.get_sink("schema") == expected_schema


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            SinksConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())
