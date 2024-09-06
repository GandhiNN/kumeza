# pylint: disable=attribute-defined-outside-init

import logging

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.config.sinks.sinks_config import Sinks
from kumeza.core.arrow import ArrowConverter
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

    def setup_metadata_attributes(self, schema_sink_id, schema_metadata_sink_id):
        # Metadata section
        self.metadata = self.ingestion_config.metadata
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
