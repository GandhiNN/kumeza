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


class PipelineFunction:

    def __init__(
        self,
        ingestion_config: IngestionConfig,
        credentials: dict,
        schema_metadata_sink_id: str,
        rawdata_metadata_sink_id: str,
    ):
        self.ingestion_config = ingestion_config
        self.credentials = credentials
        self.domain = self.ingestion_config.source_system.domain
        self.port = self.ingestion_config.source_system.port
        self.db_instance: str = self.ingestion_config.source_system.database_instance
        self.hostname = self.ingestion_config.source_system.hostname
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]
        self.source_system_id = self.ingestion_config.source_system.id
        self.source_system_physical_location = (
            self.ingestion_config.source_system.physical_location
        )
        self.tds_manager: TDSManager = TDSManager(
            self.hostname, self.port, self.db_instance
        )
        self.mssql_tds_extractor: MSSQLExtractor = MSSQLExtractor(self.tds_manager)
        self.s3: S3 = S3()
        self.dynamodb = DynamoDB()
        self.arrow_converter: ArrowConverter = ArrowConverter()
        self.dateobj: DateObject = DateObject()

        # Metadata section
        self.metadata = ingestion_config.metadata
        self.schema_metadata_sink_id = schema_metadata_sink_id
        self.rawdata_metadata_sink_id = rawdata_metadata_sink_id

        # Schema metadata section
        self.schema_metadata_table = self.ingestion_config.metadata.get_sink_target(
            self.schema_metadata_sink_id
        ).table_name
        self.schema_metadata_table_partition_key = (
            self.ingestion_config.metadata.get_sink_target(
                self.schema_metadata_sink_id
            ).partition_key
        )
        self.schema_metadata_table_sort_key = (
            self.ingestion_config.metadata.get_sink_target(
                schema_metadata_sink_id
            ).sort_key
        )
        # rawdata metadata section
        self.rawdata_metadata_table = self.ingestion_config.metadata.get_sink_target(
            self.rawdata_metadata_sink_id
        ).table_name
        self.rawdata_metadata_table_partition_key = (
            self.ingestion_config.metadata.get_sink_target(
                self.rawdata_metadata_sink_id
            ).partition_key
        )
        self.rawdata_metadata_table_sort_key = (
            self.ingestion_config.metadata.get_sink_target(
                self.rawdata_metadata_sink_id
            ).sort_key
        )

    def ingest_schema(self, ingestion_object: dict, schema_sink_id: str) -> str:
        # Set raw data flag
        raw_data_flag: str = "delta"

        # Variable expansion
        object_name = ingestion_object["table_name"].lower()
        db_name = ingestion_object["db_name"]
        sql_schema = ingestion_object["sql_statement_schema"]

        # Schema phase
        # Get schema sink
        schema_sink: Sinks = self.ingestion_config.sinks.get_sink("schema")
        schema_bucket: str = schema_sink.get_sink_target(schema_sink_id).path

        # Schema check
        rs_schema: list = self.mssql_tds_extractor.read(
            db_name, sql_schema, self.domain, self.username, self.password
        )
        arrow_rs_schema: pa.lib.Table = self.arrow_converter.from_python_list(rs_schema)
        # Get schema of the table
        schema: list[dict] = ArrowManager.get_schema(
            arrow_rs_schema, hive=True
        )  # Convert Arrow schema to Hive

        # Get schema hash
        cur_schema: str = get_schema_hash(schema)
        logger.info("Schema hash is %s", cur_schema)

        # Get the last ingestion status
        last_ing_status = self.get_last_ingestion_status(
            self.schema_metadata_table,
            object_name,
            self.schema_metadata_table_partition_key,
            self.schema_metadata_table_sort_key,
        )
        logger.info(last_ing_status)
        if len(last_ing_status["Items"]) == 0:
            logger.info("Object: %s has never been ingested", object_name)
            item = {
                f"{self.schema_metadata_table_partition_key}": f"{self.source_system_id}-{self.source_system_physical_location}-{object_name}",
                f"{self.schema_metadata_table_sort_key}": self.dateobj.get_current_timestamp(
                    ts_format="epoch"
                ),
                "schema": schema,
                "schema_hash": cur_schema,
            }
            logger.info("Registering schema of table: %s => %s", object_name, item)
            logger.info(self.dynamodb.put_item(item, self.schema_metadata_table))

            # Write schema to schema bucket
            schema_key = (
                f"""{self.source_system_id}/{self.source_system_physical_location}/"""
                f"""{object_name}/{object_name}-{self.dateobj.get_current_timestamp(ts_format="date_only")}.json"""
            )
            self.s3.write_to_bucket(
                content=schema, bucket_name=schema_bucket, key_name=schema_key
            )
        else:
            logger.info("Object: %s has been ingested before", object_name)
            # Comparing schema hash
            logger.info("Comparing schema hash for object: %s", object_name)
            prev_schema = last_ing_status["Items"][0]["schema_hash"]["S"]
            logger.info(
                "Previous schema: %s | Current schema: %s", prev_schema, cur_schema
            )
            if cur_schema != prev_schema:
                logger.info(
                    "Table: %s structure has changed",
                    object_name,
                )
                raw_data_flag = "init"
                return raw_data_flag
            logger.info(
                "Table: %s structure has not changed, continuing...", object_name
            )
        return raw_data_flag

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
        self,
        metadata_table_name: str,
        table_name: str,
        partition_key: str,
        sort_key: str,
    ):
        # Partition key = {source_system_id}-{physical_location}-{table_name}
        # Sort key = execution time in epoch
        item_name = f"{self.source_system_id}-{self.source_system_physical_location}-{table_name}"
        cur_epoch = self.dateobj.get_current_timestamp(ts_format="epoch")
        result = self.dynamodb.get_last_item_from_table(
            metadata_table_name, item_name, partition_key, sort_key, cur_epoch
        )
        return result
