# pylint: disable=attribute-defined-outside-init

import logging

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.utils.aws.dynamodb import DynamoDB
from kumeza.utils.aws.s3 import S3
from kumeza.utils.common.date_object import DateObject


logger = logging.getLogger(__name__)


class Setup:
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
        self.db_instance = self.ingestion_config.source_system.database_instance
        self.hostname = self.ingestion_config.source_system.hostname
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]
        self.source_system_id = self.ingestion_config.source_system.id
        self.source_system_physical_location = (
            self.ingestion_config.source_system.physical_location
        )
        self.s3: S3 = S3()
        self.dynamodb = DynamoDB()
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
