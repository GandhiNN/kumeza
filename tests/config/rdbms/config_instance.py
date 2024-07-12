from kumeza.config.credentials.credentials_config import CredentialsConfig
from kumeza.config.data_assets.data_assets_config import (
    Assets,
    AssetsId,
    DataAssetsConfig,
)
from kumeza.config.ingestor_config import IngestionConfig
from kumeza.config.integration.integration_config import IntegrationConfig
from kumeza.config.metadata.metadata_config import MetadataConfig
from kumeza.config.runtime_environment.runtime_environment_config import (
    RuntimeEnvironmentConfig,
)
from kumeza.config.sinks.sinks_config import Sinks, SinksConfig
from kumeza.config.source_system.source_system_config import SourceSystemConfig


class TestConfigInstanceRdbms:
    def __init__(self):
        self.runtime_environment = RuntimeEnvironmentConfig(
            id="icloud", provider="aws", service="glue", region="eu-west-1"
        )
        self.source_system = SourceSystemConfig(
            id="imel",
            env="dev",
            database_type="mssql",
            database_instance="dev",
            authentication_type="ntlm",
            hostname="sqlqa_qimel_pmhboz.dbiaas.sdi.pmi",
            domain="pmintl.net",
            port=1433,
        )
        self.integration = IntegrationConfig(
            driver="spark", connector="jdbc", fetchsize=1000, chunksize=1000000
        )
        self.credentials = CredentialsConfig(
            username="s-imel-opsdaas-qas01",
            provider="hashicorp_vault",
            workspace="icloud",
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
