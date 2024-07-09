import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.source_system.source_system_config import SourceSystemConfig
from kumeza.config.integration.integration_config import IntegrationConfig
from kumeza.config.credentials.credentials_config import CredentialsConfig
from kumeza.config.metadata.metadata_config import MetadataConfig
from kumeza.config.runtime_environment.runtime_environment_config import (
    RuntimeEnvironmentConfig,
)
from kumeza.config.sinks.sinks_config import SinksConfig
from kumeza.config.data_assets.data_assets_config import DataAssetsConfig
from kumeza.config.ingestor_config import IngestionConfig
from kumeza.config.ingestor_config import IngestionConfig

from .config_instance import TestConfigInstance

# Config files to be referenced
abs_path = os.path.dirname(__file__)
cfg_path = "files"
JSON_CONFIG = os.path.join(abs_path, cfg_path, "config.json")
YAML_CONFIG = os.path.join(abs_path, cfg_path, "config.yaml")
config_instance = TestConfigInstance()


class TestSetUp:  # pragma: no cover
    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestRuntimeEnvironmentConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        expected = config_instance.runtime_environment
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(
                self.yml_config["runtime_environment"]
            ),
            expected,
        )
        self.assertEqual(
            RuntimeEnvironmentConfig.marshal(
                self.json_config["runtime_environment"]
            ),
            expected,
        )


class TestSourceSystemConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        expected = config_instance.source_system
        self.assertEqual(
            SourceSystemConfig.marshal(self.yml_config["source_system"]),
            expected,
        )
        self.assertEqual(
            SourceSystemConfig.marshal(self.json_config["source_system"]),
            expected,
        )


class TestIntegrationConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        expected = config_instance.integration
        self.assertEqual(
            IntegrationConfig.marshal(self.yml_config["integration"]),
            expected,
        )
        self.assertEqual(
            IntegrationConfig.marshal(self.json_config["integration"]),
            expected,
        )


class TestCredentialsConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        expected = config_instance.credentials
        self.assertEqual(
            CredentialsConfig.marshal(self.yml_config["credentials"]),
            expected,
        )
        self.assertEqual(
            CredentialsConfig.marshal(self.json_config["credentials"]),
            expected,
        )


class TestMetadataConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        expected = config_instance.metadata
        self.assertEqual(
            MetadataConfig.marshal(self.yml_config["metadata"]), expected
        )
        self.assertEqual(
            MetadataConfig.marshal(self.json_config["metadata"]), expected
        )


class TestSinksConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)

    def test(self):
        expected = config_instance.sinks
        self.assertEqual(
            SinksConfig.marshal(self.yml_config["sinks"]), expected
        )
        self.assertEqual(
            SinksConfig.marshal(self.json_config["sinks"]), expected
        )


class TestDataAssetsConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)

    def test(self):
        expected = config_instance.data_assets
        self.assertEqual(
            DataAssetsConfig.marshal(self.yml_config["data_assets"]),
            expected,
        )
        self.assertEqual(
            DataAssetsConfig.marshal(self.json_config["data_assets"]),
            expected,
        )


class TestFullConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)

    def test(self):
        expected = config_instance.full_config
        self.assertEqual(IngestionConfig.marshal(self.yml_config), expected)
        self.assertEqual(IngestionConfig.marshal(self.json_config), expected)


def suite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestRuntimeEnvironmentConfig,
            TestSourceSystemConfig,
            TestIntegrationConfig,
            TestCredentialsConfig,
            TestMetadataConfig,
            TestSinksConfig,
            TestDataAssetsConfig,
            TestFullConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())  # pragma: no cover
