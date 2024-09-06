# pylint: disable=attribute-defined-outside-init

import logging

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.config.sinks.sinks_config import Sinks
from kumeza.core.arrow import ArrowConverter
from kumeza.utils.aws.dynamodb.dynamodb import DynamoDB
from kumeza.utils.aws.s3.s3 import S3
from kumeza.utils.common.date_object import DateObject
from kumeza.utils.query.mssql import MSSQLQueryTemplater


logger = logging.getLogger(__name__)


class Pipeline:

    def __init__(self, ingestion_config: IngestionConfig, credentials: dict):
        self.ingestion_config = ingestion_config
        self.credentials = credentials

    def setup(self):
        # Setup helper methods
        self.s3: S3 = S3()
        self.dynamodb = DynamoDB()
        self.arrow_converter: ArrowConverter = ArrowConverter()
        self.dateobj: DateObject = DateObject()

        # Setup attributes
        self.domain = self.ingestion_config.source_system.domain
        self.port = self.ingestion_config.source_system.port
        self.db_instance = self.ingestion_config.source_system.database_instance
        self.hostname = self.ingestion_config.source_system.hostname
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]
        self.source_system_id = self.ingestion_config.source_system.id
        self.source_system_physical_location = (
            self.ingestion_config.source_system.physical_location
        )
        self.metadata = self.ingestion_config.metadata

    def setup_ingestion_objects(self):
        self.data_assets = self.ingestion_config.data_assets
        self.ingestion_objects = []
        self.ingestion_objects_furnished = []
        for asset_id in self.data_assets.id:
            for asset in asset_id.assets:
                mssql_query_templater = MSSQLQueryTemplater(
                    self.ingestion_config.source_system, asset
                )
                sql_statement_schema = mssql_query_templater.get_sql_query(
                    mode="schema"
                )
                sql_statement_raw = mssql_query_templater.get_sql_query(mode="standard")
                self.ingestion_objects.append(
                    {
                        "table_name": asset.asset_name,
                        "db_name": asset.database_name,
                        "sql_statement_schema": sql_statement_schema,
                        "sql_statement_raw": sql_statement_raw,
                    }
                )

    def setup_schema_metadata_attributes(self, schema_sink_id, schema_metadata_sink_id):
        # Metadata section
        self.schema_sink: Sinks = self.ingestion_config.sinks.get_sink("schema")
        self.schema_bucket: str = self.schema_sink.get_sink_target(schema_sink_id).path

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
        self.raw_data_sink: Sinks = self.ingestion_config.sinks.get_sink("raw")
        self.raw_data_bucket: str = self.raw_data_sink.get_sink_target(
            raw_data_sink_id
        ).path

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

    def get_last_ingestion_status(self, table_name: str):
        # Partition key = {source_system_id}-{physical_location}-{table_name}
        # Sort key = execution time in epoch
        item_name = f"{self.source_system_id}-{self.source_system_physical_location}-{table_name}"
        cur_epoch = self.dateobj.get_current_timestamp(ts_format="epoch")
        result = self.dynamodb.get_last_item_from_table(
            self.schema_metadata_table,
            item_name,
            self.schema_metadata_table_partition_key,
            self.schema_metadata_table_sort_key,
            cur_epoch,
        )
        return result
