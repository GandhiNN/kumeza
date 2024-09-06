# pylint: disable=attribute-defined-outside-init

import logging

from kumeza.connectors.tds import TDSManager
from kumeza.extractors.mssql import MSSQLExtractor
from kumeza.pipeline.mssql_tds.pipeline import Pipeline


logger = logging.getLogger(__name__)


class Runner:

    def __init__(self, hostname, port, db_instance):
        self.tds_manager = TDSManager(hostname, port, db_instance)
        self.extractor: MSSQLExtractor = MSSQLExtractor(self.tds_manager)

    def get_schema_sequential(self, schema_sink_id, schema_metadata_sink_id):

        # Setup metadata attributes
        self.pipeline.setup_metadata_attributes(schema_sink_id, schema_metadata_sink_id)

        # loop through ingestion object
        for obj in self.pipeline.ingestion_objects:
            object_name = obj["table_name"].lower()
            db_name = obj["db_name"]
            sql_schema = obj["sql_statement_schema"]
            print(object_name, db_name, sql_schema)

            resp = self.pipeline.get_last_ingestion_status(object_name)
            print(resp, type(resp))
            # self.pipeline.ingest_schema()
            # self.pipeline.ingest_raw_data()

    def run(
        self,
        ingestion_config,
        credentials,
        schema_sink_id,
        schema_metadata_sink_id,
        concurrent=False,
    ):
        # Setup basic pipeline attributes
        self.pipeline = Pipeline(ingestion_config, credentials)
        self.pipeline.setup()
        self.pipeline.setup_ingestion_objects()
        logger.info(
            "Processing %s numbers of ingestion objects",
            len(self.pipeline.ingestion_objects),
        )
        if concurrent is False:
            self.get_schema_sequential(schema_sink_id, schema_metadata_sink_id)
        print("Done")
