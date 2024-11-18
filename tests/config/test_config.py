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
from kumeza.config.integration.rest import RestConfig
from kumeza.config.loader import ConfigLoader
from kumeza.config.metadata.metadata_config import Metadata, MetadataConfig
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
            id="aws_tenant",
            provider="aws",
            service="glue",
            region="ap-southeast-3",
            env="dev",
        )
        self.source_system = SourceSystemConfig(
            id="custdb",
            type="rdbms",
            database_engine="mssql",
            database_instance="dev",
            authentication_type="ntlm",
            hostname="custdb.mssql.example",
            domain="example.com",
            port=1433,
            physical_location="jkt",
        )
        self.integration = IntegrationConfig(
            engine="spark", driver="jdbc", fetchsize=1000, chunksize=1000000
        )
        self.rest = RestConfig(
            base_url="http://example.com",
            header={"Accept": "application/csv", "Content-Type": "multipart/form-data"},
            params={"chunked": True, "chunk_size": 2000},
        )
        self.credentials = CredentialsConfig(
            username="some_username",
            provider="hashicorp_vault",
            url="https://hashicorp-vault.dev.com:8200",
            verify_ssl=False,
            namespace="some_vault_namespace",
            secret_name="hcv-approle-dev",
            mount_point="static-secret",
            path="data/custdb",
        )
        self.metadata = MetadataConfig(
            metadata_targets=[
                Metadata(
                    metadata_type="ingestion_status",
                    sink_target="dynamodb",
                    table_name="ingestion-status-dev",
                    partition_key="ingestor_name",
                    sort_key="execution_time",
                ),
                Metadata(
                    metadata_type="schema_registrar",
                    sink_target="dynamodb",
                    table_name="schema-status-dev",
                    partition_key="ingestor_name",
                    sort_key="execution_time",
                ),
            ]
        )
        self.sinks = SinksConfig(
            [
                Sinks(
                    sink_type="raw",
                    sink_targets=[
                        SinkTargets(
                            id="staging",
                            target="s3",
                            file_format="parquet",
                            path="staging-bucket-dev",
                        ),
                        SinkTargets(
                            id="silver",
                            target="s3",
                            file_format="parquet",
                            path="silver-bucket-dev",
                        ),
                    ],
                ),
                Sinks(
                    sink_type="schema",
                    sink_targets=[
                        SinkTargets(
                            id="raw_schema",
                            target="s3",
                            file_format="json",
                            path="schema-bucket-dev",
                        )
                    ],
                ),
            ]
        )
        self.data_assets = DataAssetsConfig(
            [
                AssetsId(
                    id="group_1",
                    database_name="master",
                    assets=[
                        Assets(
                            asset_name="CUSTOMER_ID",
                            asset_type="table",
                            database_schema="dbo",
                            query_type="standard",
                            incremental=True,
                            incremental_column="updated_at",
                            reload=False,
                            partition_columns=[],
                            columns_to_anonymize=[],
                            custom_query=None,
                            custom_schema={},
                            cast_timestamp_columns_to_string=False,
                        ),
                        Assets(
                            asset_name="CUSTOMER_NAME",
                            asset_type="table",
                            database_schema="dbo",
                            query_type="standard",
                            incremental=False,
                            incremental_column="",
                            reload=False,
                            partition_columns=[],
                            columns_to_anonymize=[],
                            custom_query=None,
                            custom_schema={},
                            cast_timestamp_columns_to_string=False,
                        ),
                    ],
                ),
                AssetsId(
                    id="group_2",
                    database_name="master",
                    assets=[
                        Assets(
                            asset_name="ADDRESS",
                            asset_type="table",
                            database_schema="dbo",
                            query_type="standard",
                            incremental=False,
                            incremental_column="created_at",
                            reload=False,
                            partition_columns=[],
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
            self.rest,
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
