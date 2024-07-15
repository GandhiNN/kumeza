import os
import unittest

from kumeza.config.data_assets.data_assets_config import DataAssetsConfig
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


class ConfigInstanceDataAssets:
    def __init__(self):
        self.data_assets_plain_yaml = ConfigLoader.load(YAML_CONFIG)["data_assets"]
        self.data_assets_plain_json = ConfigLoader.load(JSON_CONFIG)["data_assets"]


class DataAssetsConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        # Set base config object
        base_config = ConfigInstanceDataAssets()
        outer_keys_yaml = list(base_config.data_assets_plain_yaml[0].keys())
        outer_keys_json = list(base_config.data_assets_plain_json[0].keys())

        # Keys sameness assertion
        self.assertEqual(
            DataAssetsConfig.marshal(self.yml_config["data_assets"])
            .id[0]
            .get_field_name(),  # Test against AssetsId dataclass
            outer_keys_yaml,
        )

        self.assertEqual(
            DataAssetsConfig.marshal(self.json_config["data_assets"])
            .id[0]
            .get_field_name(),  # Test against AssetsId dataclass
            outer_keys_json,
        )

        # Object length assertion
        self.assertEqual(
            DataAssetsConfig.marshal(self.yml_config["data_assets"])
            .id[0]
            .get_length(),  # Test against AssetsId dataclass
            len(outer_keys_yaml),
        )
        self.assertEqual(
            DataAssetsConfig.marshal(self.json_config["data_assets"])
            .id[0]
            .get_length(),  # Test against AssetsId dataclass
            len(outer_keys_json),
        )


class AssetsConfigTest(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        # Set base config object
        base_config = ConfigInstanceDataAssets()
        outer_keys_yaml = list(
            base_config.data_assets_plain_yaml[0]["assets"][0].keys()
        )
        outer_keys_json = list(
            base_config.data_assets_plain_json[0]["assets"][0].keys()
        )

        # Keys sameness assertion
        self.assertEqual(
            DataAssetsConfig.marshal(self.yml_config["data_assets"])
            .id[0]
            .assets[0]
            .get_field_name(),  # Test against AssetsId dataclass
            outer_keys_yaml,
        )

        self.assertEqual(
            DataAssetsConfig.marshal(self.json_config["data_assets"])
            .id[0]
            .assets[0]
            .get_field_name(),  # Test against AssetsId dataclass
            outer_keys_json,
        )

        # Object length assertion
        self.assertEqual(
            DataAssetsConfig.marshal(self.yml_config["data_assets"])
            .id[0]
            .assets[0]
            .get_length(),  # Test against AssetsId dataclass
            len(outer_keys_yaml),
        )
        self.assertEqual(
            DataAssetsConfig.marshal(self.json_config["data_assets"])
            .id[0]
            .assets[0]
            .get_length(),  # Test against AssetsId dataclass
            len(outer_keys_json),
        )


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        [
            unittest.TestLoader().loadTestsFromTestCase(DataAssetsConfigTest),
            unittest.TestLoader().loadTestsFromTestCase(AssetsConfigTest),
        ]
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())
