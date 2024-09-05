import logging

from kumeza.config.ingestor_config import IngestionConfig
from kumeza.core.multithreader import MultithreadingManager
from kumeza.pipeline.mssql_tds.wrapper.pipeline import PipelineFunction


logger = logging.getLogger(__name__)


class MSSQLConcurrentRunner(PipelineFunction):

    def __init__(
        self,
        ingestion_config: IngestionConfig,
        credentials: dict,
        schema_metadata_sink_id: str,
        raw_metadata_sink_id: str,
    ):
        super().__init__(
            ingestion_config, credentials, schema_metadata_sink_id, raw_metadata_sink_id
        )
        self.multithreading_manager = MultithreadingManager(worker_numbers=10)

    def execute(
        self,
        task: str,
        ingestion_objects: list[dict],
        schema_sink_id: str,
        raw_sink_id: str,
    ):
        if task == "schema":
            self.multithreading_manager.execute(
                self.ingest_schema,
                ingestion_objects,
                schema_sink_id,
                self.schema_metadata_sink_id,
            )
        if task == "raw":
            self.multithreading_manager.execute(
                self.ingest_raw,
                ingestion_objects,
                raw_sink_id,
                self.rawdata_metadata_sink_id,
            )
