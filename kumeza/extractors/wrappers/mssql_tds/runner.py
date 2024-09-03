import logging

import pyarrow as pa

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.config.sinks.sinks_config import Sinks
from kumeza.connectors.tds import TDSManager
from kumeza.core.arrow import ArrowConverter, ArrowManager
from kumeza.core.data import get_schema_hash
from kumeza.extractors.mssql import MSSQLExtractor
from kumeza.utils.aws.dynamodb.dynamodb import DynamoDB
from kumeza.utils.aws.s3.s3 import S3
from kumeza.utils.common.date_object import DateObject


logger = logging.getLogger(__name__)


class MSSQLRunner:

    def __init__(
        self,
        ingestion_config: IngestionConfig,
        credentials: dict,
    ):
        self.ingestion_config = ingestion_config
        self.credentials = credentials
        self.domain = self.ingestion_config.source_system.domain
        self.port = self.ingestion_config.source_system.port
        self.db_instance: str = self.ingestion_config.source_system.database_instance
        self.hostname = self.ingestion_config.source_system.hostname
        self.tds_manager: TDSManager = TDSManager(
            self.hostname, self.port, self.db_instance
        )
        self.mssql_tds_extractor: MSSQLExtractor = MSSQLExtractor(self.tds_manager)
        self.s3: S3 = S3()
        self.dynamodb = DynamoDB()
        self.arrow_converter: ArrowConverter = ArrowConverter()
        self.dateobj: DateObject = DateObject()
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]
        self.source_system_id = self.ingestion_config.source_system.id
        self.source_system_physical_location = (
            self.ingestion_config.source_system.physical_location
        )

    def ingest_schema(self, ingestion_object: dict, schema_sink_id: str):

        # Variable expansion
        table_name = ingestion_object["table_name"].lower()
        db_name = ingestion_object["db_name"]
        sql_schema = ingestion_object["sql_statement_schema"]

        # Schema phase
        # Get schema sink
        schema_sink: Sinks = self.ingestion_config.sinks.get_sink("schema")
        schema_bucket: str = schema_sink.get_sink_target(schema_sink_id).path

        rs_schema: list = self.mssql_tds_extractor.read(
            db_name, sql_schema, self.domain, self.username, self.password
        )
        arrow_rs_schema: pa.lib.Table = self.arrow_converter.from_python_list(rs_schema)
        # Get schema of the table
        schema: list[dict] = ArrowManager.get_schema(
            arrow_rs_schema, hive=True
        )  # Convert Arrow schema to Hive
        # Get schema hash
        schema_hash: str = get_schema_hash(schema)
        logger.info("Schema hash is %s", schema_hash)
        # Write schema to schema bucket
        schema_key = (
            f"""{self.source_system_id}/{self.source_system_physical_location}/"""
            f"""{table_name}/{table_name}-{self.dateobj.get_current_timestamp(ts_format="date_only")}.json"""
        )
        self.s3.write_to_bucket(
            content=schema, bucket_name=schema_bucket, key_name=schema_key
        )

    # Raw data ingestion phase
    def ingest_raw(self, ingestion_object: dict, raw_sink_id: str):
        table_name = ingestion_object["table_name"].lower()
        db_name = ingestion_object["db_name"]
        sql_raw = ingestion_object["sql_statement_raw"]
        # Get raw data sink
        raw_data_sink: Sinks = self.ingestion_config.sinks.get_sink("raw")
        raw_data_bucket: str = raw_data_sink.get_sink_target(raw_sink_id).path
        # Raw data phase
        rs_raw: list = self.mssql_tds_extractor.read(
            db_name, sql_raw, self.domain, self.username, self.password
        )
        arrow_result_sets_raw: pa.lib.Table = self.arrow_converter.from_python_list(
            rs_raw
        )
        # Write raw to raw bucket
        raw_key = (
            f"""{self.source_system_id}/{self.source_system_physical_location}/"""
            f"""{table_name}/{self.dateobj.get_current_timestamp(ts_format="date_only")}/"""
        )
        ArrowManager.write_to_s3(
            arrow_result_sets_raw, f"s3://{raw_data_bucket}/{raw_key}"
        )

    def get_last_ingestion_status(
        self, table_name: str, partition_key: str, sort_key: str
    ):
        # Partition key = {source_system_id}-{physical_location}-{table_name}
        # Sort key = execution time in epoch
        item_name = f"{self.source_system_id}-{self.source_system_physical_location}-{table_name}"
        cur_epoch = self.dateobj.get_current_timestamp(ts_format="epoch")
        return self.dynamodb.get_last_item_from_table(
            table_name, item_name, partition_key, sort_key, cur_epoch
        )
