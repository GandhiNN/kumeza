import logging

from kumeza.pipeline.mssql_tds.wrapper.pipeline import PipelineFunction


logger = logging.getLogger(__name__)


class MSSQLSequentialRunner(PipelineFunction):

    def execute(
        self,
        task: str,
        ingestion_objects: list[dict],
        schema_sink_id: str,
        raw_sink_id: str,
    ):
        if task == "schema":
            for t_object in ingestion_objects:
                self.ingest_schema(t_object, schema_sink_id)
        if task == "raw":
            for t_object in ingestion_objects:
                self.ingest_raw(t_object, raw_sink_id)
