import logging

from kumeza.pipeline.mssql_tds.wrapper.func import Func


logger = logging.getLogger(__name__)


class MSSQLSequentialRunner(Func):

    def execute(
        self,
        task: str,
        ingestion_objects: list[dict],
        schema_sink_id: str,
        raw_sink_id: str,
        ing_metadata_id: str,
    ):
        if task == "schema":
            for t_object in ingestion_objects:
                self.ingest_schema(t_object, schema_sink_id, ing_metadata_id)
        if task == "raw":
            for t_object in ingestion_objects:
                self.ingest_raw(t_object, raw_sink_id)
