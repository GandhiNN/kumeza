import os
import unittest

from kumeza.config.credentials.credentials_config import CredentialsConfig
from kumeza.config.data_assets.data_assets_config import (
    Assets,
    AssetsId,
    DataAssetsConfig,
)
from kumeza.config.ingestor_config import IngestionConfig
from kumeza.config.integration.integration_config import IntegrationConfig
from kumeza.config.loader import ConfigLoader
from kumeza.config.metadata.metadata_config import MetadataConfig
from kumeza.config.runtime_environment.runtime_environment_config import (
    RuntimeEnvironmentConfig,
)
from kumeza.config.sinks.sinks_config import Sinks, SinksConfig
from kumeza.config.source_system.source_system_config import SourceSystemConfig


# Config files to be referenced
abs_path = os.path.dirname(__file__)
cfg_path = "files"
JSON_CONFIG = os.path.join(abs_path, cfg_path, "config.json")
YAML_CONFIG = os.path.join(abs_path, cfg_path, "config.yaml")


class TestConfigInstance:
    def __init__(self):
        self.runtime_environment = RuntimeEnvironmentConfig(
            id="doadi", provider="aws", service="glue", region="eu-west-1", env="dev"
        )
        self.source_system = SourceSystemConfig(
            id="spa",
            database_type="mssql",
            database_instance="PRD",
            authentication_type="ntlm",
            hostname="pmichlausql276.pmintl.net",
            domain="PMINTL.NET",
            port=49600,
        )
        self.integration = IntegrationConfig(
            engine="spark", driver="jdbc", fetchsize=1000, chunksize=1000000
        )
        self.credentials = CredentialsConfig(
            username="s-imel-opsdaas-qas01",
            provider="hashicorp vault",
            url="https://vault.vault-dev-dev.shared-services.eu-west-1.aws.pmicloud.biz:8200",
            verify_ssl=False,
            namespace="doadi",
            mount_point="static-secret",
            path="data/spa",
        )
        self.metadata = MetadataConfig(
            sink_type="dynamodb",
            table_name="el-doadi-flexible-ingestion-spa-ing-ingestion-status-prd",
        )
        self.sinks = SinksConfig(
            [
                Sinks(
                    id="doadi raw bucket",
                    target="s3",
                    file_format="parquet",
                    path="el-doadi-flexible-ingestion-spa-raw-bucket-prd",
                ),
                Sinks(
                    id="enterprise landing raw bucket",
                    target="s3",
                    file_format="parquet",
                    path="enterprise-landing-raw-prd",
                ),
                Sinks(
                    id="enterprise landing schema bucket",
                    target="s3",
                    file_format="json",
                    path="enterprise-landing-schema-raw-prd",
                ),
            ]
        )
        self.data_assets = DataAssetsConfig(
            [
                AssetsId(
                    id="group_1",
                    assets=[
                        Assets(
                            asset_name="tbl_lines",
                            asset_type="table",
                            database_name="SPA_reporting",
                            database_schema="dbo",
                            query_type="standard",
                            reload=False,
                            record_creation_columns=[],
                            record_update_columns=[],
                            columns_to_anonymize=[],
                            custom_query=None,
                            custom_schema={},
                            cast_timestamp_columns_to_string=False,
                        ),
                        Assets(
                            asset_name="tbl_functionallocationrelation",
                            asset_type="table",
                            database_name="SPA_reporting",
                            database_schema="dbo",
                            query_type="standard",
                            reload=False,
                            record_creation_columns=[],
                            record_update_columns=[],
                            columns_to_anonymize=[],
                            custom_query=None,
                            custom_schema={},
                            cast_timestamp_columns_to_string=False,
                        ),
                    ],
                ),
                AssetsId(
                    id="group_2",
                    assets=[
                        Assets(
                            asset_name="tbl_fl_speed_pershiftpo",
                            asset_type="table",
                            database_name="SPA_reporting",
                            database_schema="dbo",
                            query_type="standard",
                            reload=False,
                            record_creation_columns=[],
                            record_update_columns=[],
                            columns_to_anonymize=[],
                            custom_query=None,
                            custom_schema={},
                            cast_timestamp_columns_to_string=False,
                        )
                    ],
                ),
            ]
        )
        self.full_config = IngestionConfig(
            self.runtime_environment,
            self.source_system,
            self.integration,
            self.credentials,
            self.metadata,
            self.sinks,
            self.data_assets,
        )


class TestFullConfig(unittest.TestCase):
    def setUp(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)
        self.config_instance = TestConfigInstance()

    def test(self):
        expected = self.config_instance.full_config
        self.assertEqual(IngestionConfig.marshal(self.yml_config), expected)
        self.assertEqual(IngestionConfig.marshal(self.json_config), expected)


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestFullConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
