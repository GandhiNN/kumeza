import logging

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.core.multithreader import MultithreadingManager
from kumeza.pipeline.mssql_tds.runner import MSSQLRunner


logger = logging.getLogger(__name__)


class MSSQLConcurrentRunner(MSSQLRunner):

    def __init__(
        self,
        multithreading_manager: MultithreadingManager,
        ingestion_config: IngestionConfig,
        credentials: dict,
    ):
        super().__init__(ingestion_config, credentials)
        self.multithreading_manager = multithreading_manager

    def execute(
        self,
        task: str,
        ingestion_objects: list[dict],
        schema_sink_id: str,
        raw_sink_id: str,
        ing_metadata_id: str,
    ):
        if task == "schema":
            self.multithreading_manager.execute(
                self.ingest_schema, ingestion_objects, schema_sink_id, ing_metadata_id
            )
        if task == "raw":
            self.multithreading_manager.execute(
                self.ingest_raw, ingestion_objects, raw_sink_id
            )
