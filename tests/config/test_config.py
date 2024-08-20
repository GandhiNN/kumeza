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
from kumeza.config.sinks.sinks_config import Sinks, SinksConfig, SinkTargets
from kumeza.config.source_system.source_system_config import SourceSystemConfig


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
CFG_PATH = "files"
JSON_CONFIG = os.path.join(ABS_PATH, CFG_PATH, "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, CFG_PATH, "config.yaml")


class ConfigInstance:
    def __init__(self):
        self.runtime_environment = RuntimeEnvironmentConfig(
            id="icloud", provider="aws", service="glue", region="eu-west-1", env="dev"
        )
        self.source_system = SourceSystemConfig(
            id="imel",
            database_engine="mssql",
            database_instance="dev",
            authentication_type="ntlm",
            hostname="sqlqa_qimel_pmhboz.dbiaas.sdi.pmi",
            domain="pmintl.net",
            port=1433,
        )
        self.integration = IntegrationConfig(
            engine="spark", driver="jdbc", fetchsize=1000, chunksize=1000000
        )
        self.credentials = CredentialsConfig(
            username="s-imel-opsdaas-qas01",
            provider="hashicorp_vault",
            url="https://vault.vault-dev-dev.shared-services.eu-west-1.aws.pmicloud.biz:8200",
            verify_ssl=False,
            namespace="icloud",
            secret_name="hcv-icloud-approle-dev",
            mount_point="static-secret",
            path="data/imel",
        )
        self.metadata = MetadataConfig(
            sink_type="dynamodb",
            table_name="daas-imel-ingestion-status-dev",
        )
        self.sinks = SinksConfig(
            [
                Sinks(
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
                ),
                Sinks(
                    sink_type="schema",
                    sink_targets=[
                        SinkTargets(
                            id="enterprise_landing_schema_bucket",
                            target="s3",
                            file_format="json",
                            path="enterprise-landing-schema-raw-dev",
                        )
                    ],
                ),
            ]
        )
        self.data_assets = DataAssetsConfig(
            [
                AssetsId(
                    id="group_1",
                    assets=[
                        Assets(
                            asset_name="ACTION_FLAG_TYPE",
                            asset_type="table",
                            database_name="Apriso",
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
                            asset_name="ACTION_SCRIPT",
                            asset_type="table",
                            database_name="Apriso",
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
                            asset_name="WIP_ORDER",
                            asset_type="table",
                            database_name="Apriso",
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


class FullConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)
        self.config_instance = ConfigInstance()

    def test(self):
        expected = self.config_instance.full_config
        self.assertEqual(IngestionConfig.marshal(self.yml_config), expected)
        self.assertEqual(IngestionConfig.marshal(self.json_config), expected)


def testSuite():
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            FullConfigTest,
        )
    )


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())
