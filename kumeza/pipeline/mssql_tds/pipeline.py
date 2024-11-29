# pylint: disable=attribute-defined-outside-init

import logging

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.connectors.tds import TDSManager
from kumeza.core.arrow import ArrowIO, ArrowUtils
from kumeza.extractors.mssql import MSSQLExtractor
from kumeza.query.mssql import MSSQLQueryTemplater
from kumeza.utils.aws.dynamodb.dynamodb import DynamoDB
from kumeza.utils.aws.s3.s3 import S3
from kumeza.utils.common.date_object import DateObject


logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, ingestion_config: IngestionConfig, credentials: dict):
        self.ingestion_config = ingestion_config
        self.credentials = credentials

    def setup(self):
        # Setup helper methods
        self.s3 = S3()
        self.dynamodb = DynamoDB()
        self.arrow_utils = ArrowUtils()
        self.arrow_io = ArrowIO
        self.dateobj = DateObject()

        # Setup attributes
        self.env = self.ingestion_config.runtime_environment.env
        self.domain = self.ingestion_config.source_system.domain
        self.hostname = self.ingestion_config.source_system.hostname
        self.port = self.ingestion_config.source_system.port
        self.db_instance = self.ingestion_config.source_system.database_instance
        self.authentication_type = (
            self.ingestion_config.source_system.authentication_type
        )
        self.source_system_id = self.ingestion_config.source_system.id
        self.source_system_physical_location = (
            self.ingestion_config.source_system.physical_location
        )
        self.metadata = self.ingestion_config.metadata
        self.data_assets = self.ingestion_config.data_assets
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]

    def setup_query_engine(self):
        # Setup query templater
        self.mssql_query_templater = MSSQLQueryTemplater(
            self.ingestion_config.source_system
        )
        # Initiate connector and extractor object
        self.tds_manager = TDSManager(
            hostname=self.hostname,
            port=self.port,
            db_instance=self.db_instance,
            authentication_type=self.authentication_type,
        )
        self.extractor = MSSQLExtractor(self.tds_manager)

    def setup_ingestion_objects(self) -> list:
        ingestion_objects = []
        for asset_id in self.data_assets.id:
            for asset in asset_id.assets:
                # Schema ingestion
                sql_statement_schema = self.mssql_query_templater.get_query(
                    mode="schema",
                    database_name=asset_id.database_name,
                    database_schema=asset.database_schema,
                    object_name=asset.asset_name,
                )
                # Raw data ingestion
                sql_statement_raw = self.mssql_query_templater.get_query(
                    mode="standard",
                    database_name=asset_id.database_name,
                    database_schema=asset.database_schema,
                    object_name=asset.asset_name,
                )
                # Row count ingestion
                sql_row_count = self.mssql_query_templater.get_query(
                    mode="row_count",
                    database_name=asset_id.database_name,
                    database_schema=asset.database_schema,
                    object_name=asset.asset_name,
                )
                ingestion_objects.append(
                    {
                        "table_name": asset.asset_name,
                        "db_name": asset_id.database_name,
                        "db_schema": asset.database_schema,
                        "incremental_col": asset.incremental_column,
                        "sql_statement_schema": sql_statement_schema,
                        "sql_statement_raw": sql_statement_raw,
                        "sql_row_count": sql_row_count,
                    }
                )
        return ingestion_objects

    def setup_schema_metadata_attributes(self, schema_sink_id, schema_metadata_sink_id):
        # Metadata section
        self.schema_sink = self.ingestion_config.sinks.get_sink("schema")
        self.schema_bucket = self.schema_sink.get_sink_target(schema_sink_id).path

        # Schema metadata section
        self.schema_metadata_table = self.ingestion_config.metadata.get_sink_target(
            schema_metadata_sink_id
        ).table_name
        self.schema_metadata_table_partition_key = (
            self.ingestion_config.metadata.get_sink_target(
                schema_metadata_sink_id
            ).partition_key
        )
        self.schema_metadata_table_sort_key = (
            self.ingestion_config.metadata.get_sink_target(
                schema_metadata_sink_id
            ).sort_key
        )

    def setup_raw_data_metadata_attributes(
        self, raw_data_sink_id, raw_data_metadata_sink_id
    ):
        # Metadata section
        self.raw_data_sink = self.ingestion_config.sinks.get_sink("raw")
        self.raw_data_bucket = self.raw_data_sink.get_sink_target(raw_data_sink_id).path

        # raw data metadata section
        self.raw_data_metadata_table = self.ingestion_config.metadata.get_sink_target(
            raw_data_metadata_sink_id
        ).table_name
        self.raw_data_metadata_table_partition_key = (
            self.ingestion_config.metadata.get_sink_target(
                raw_data_metadata_sink_id
            ).partition_key
        )
        self.raw_data_metadata_table_sort_key = (
            self.ingestion_config.metadata.get_sink_target(
                raw_data_metadata_sink_id
            ).sort_key
        )

    def get_last_ingestion_status(
        self,
        metadata_table_name: str,
        partition_key: str,
        sort_key: str,
        object_name: str,
    ):
        # Partition key = {source_system_id}-{physical_location}-{table_name}
        # Sort key = execution time in epoch
        item_name = f"{self.source_system_id}-{self.source_system_physical_location}-{object_name}"
        cur_epoch = self.dateobj.get_timestamp_as_str(ts_format="epoch")
        result = self.dynamodb.get_last_item_from_table(
            metadata_table_name,
            item_name,
            partition_key,
            sort_key,
            cur_epoch,
        )
        return result
